"""Dynamic reverse proxy / L7 load balancer.

Refreshes a routing table from the control plane every few seconds
(deployment name -> healthy replica endpoints) and forwards inbound requests:

    http://<proxy>/<deployment-name>/<path>  ->  http://<replica-host:port>/<path>

Traffic is spread round-robin across a deployment's healthy replicas. Because
the control plane only advertises RUNNING replicas on HEALTHY nodes, scaling and
self-healing show up here automatically — no proxy restart needed. If a chosen
backend refuses a connection, the proxy fails over to the next replica.
"""
from __future__ import annotations

import asyncio
import itertools
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from common.config import ProxySettings
from common.schemas import RouteEntry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("mc.proxy")

settings = ProxySettings()

# Hop-by-hop headers must not be forwarded (RFC 7230 §6.1).
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}


class RoutingTable:
    def __init__(self) -> None:
        self._routes: dict[str, list[str]] = {}
        self._rr: dict[str, itertools.cycle] = {}
        self._lock = asyncio.Lock()

    async def update(self, entries: list[RouteEntry]) -> None:
        async with self._lock:
            new: dict[str, list[str]] = {e.name: list(e.endpoints) for e in entries}
            self._routes = new
            # Reset round-robin cursors, keeping ones whose endpoints are unchanged.
            self._rr = {name: itertools.cycle(eps) for name, eps in new.items() if eps}

    async def endpoints(self, name: str) -> list[str]:
        async with self._lock:
            return list(self._routes.get(name, []))

    async def next_start(self, name: str) -> int:
        """Advance the round-robin cursor and return an offset into endpoints()."""
        async with self._lock:
            cyc = self._rr.get(name)
            eps = self._routes.get(name, [])
            if not cyc or not eps:
                return 0
            target = next(cyc)
            try:
                return eps.index(target)
            except ValueError:
                return 0

    async def snapshot(self) -> dict[str, list[str]]:
        async with self._lock:
            return {k: list(v) for k, v in self._routes.items()}


class RouteRefresher:
    def __init__(self, table: RoutingTable) -> None:
        self.table = table
        self._client = httpx.AsyncClient(timeout=5.0)
        self._task: asyncio.Task | None = None
        self._stopping = False

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._client.aclose()

    async def _run(self) -> None:
        while not self._stopping:
            try:
                resp = await self._client.get(f"{settings.control_plane_url}/routes")
                resp.raise_for_status()
                entries = [RouteEntry.model_validate(x) for x in resp.json()]
                await self.table.update(entries)
            except Exception as exc:  # noqa: BLE001
                log.warning("route refresh failed: %s", exc)
            await asyncio.sleep(settings.refresh_interval_s)


table = RoutingTable()
refresher = RouteRefresher(table)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(timeout=settings.upstream_timeout_s)
    await refresher.start()
    log.info("proxy up on %s:%s -> control plane %s",
             settings.host, settings.port, settings.control_plane_url)
    yield
    await refresher.stop()
    await app.state.client.aclose()


app = FastAPI(title="Mini Cloud — Reverse Proxy", lifespan=lifespan)


@app.get("/__mc/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/__mc/routes")
async def show_routes() -> dict:
    return await table.snapshot()


async def _forward(request: Request, name: str, upstream_path: str) -> Response:
    endpoints = await table.endpoints(name)
    if not endpoints:
        return JSONResponse({"error": f"no healthy backends for '{name}'"}, status_code=503)

    start = await table.next_start(name)
    body = await request.body()
    fwd_headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
    client_host = request.client.host if request.client else "unknown"
    xff = request.headers.get("x-forwarded-for")
    fwd_headers["x-forwarded-for"] = f"{xff}, {client_host}" if xff else client_host

    client: httpx.AsyncClient = app.state.client
    last_error = None
    # Try each backend once, starting at the round-robin offset (fail-over).
    for i in range(len(endpoints)):
        ep = endpoints[(start + i) % len(endpoints)]
        url = f"http://{ep}/{upstream_path}"
        try:
            upstream = await client.request(
                request.method, url,
                headers=fwd_headers, content=body,
                params=request.query_params,
            )
        except httpx.HTTPError as exc:
            last_error = exc
            log.warning("backend %s for '%s' failed: %s", ep, name, exc)
            continue
        resp_headers = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP}
        resp_headers["x-mc-backend"] = ep
        return Response(content=upstream.content, status_code=upstream.status_code,
                        headers=resp_headers)

    return JSONResponse(
        {"error": f"all backends for '{name}' failed", "detail": str(last_error)},
        status_code=502,
    )


ALL_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


@app.api_route("/{name}", methods=ALL_METHODS)
async def proxy_root(name: str, request: Request) -> Response:
    return await _forward(request, name, "")


@app.api_route("/{name}/{upstream_path:path}", methods=ALL_METHODS)
async def proxy_path(name: str, upstream_path: str, request: Request) -> Response:
    return await _forward(request, name, upstream_path)
