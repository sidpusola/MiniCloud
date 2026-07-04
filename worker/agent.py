"""Worker agent: one per machine.

Runs a small HTTP command API the control plane calls to start/stop containers,
plus a background loop that registers with the control plane and then heartbeats
resource metrics + container status on a fixed interval. Missing heartbeats are
how the control plane detects this node has failed.
"""
from __future__ import annotations

import asyncio
import logging
import socket
from contextlib import asynccontextmanager

import httpx
import psutil
from fastapi import FastAPI, HTTPException

from common.config import WorkerSettings
from common.schemas import (
    ContainerReport,
    Heartbeat,
    ReplicaStatus,
    StartContainerCommand,
    StartContainerResult,
    StopContainerCommand,
)
from worker.docker_manager import DockerManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("mc.worker")

settings = WorkerSettings()
NODE_ID = settings.node_id or f"node-{socket.gethostname()}"
ADVERTISE_PORT = settings.advertise_port or settings.port


def _collect_metrics() -> dict:
    cores = float(psutil.cpu_count() or 1)
    util = psutil.cpu_percent(interval=None) / 100.0   # since last call
    vm = psutil.virtual_memory()
    return {
        "cpu_total": cores,
        "cpu_available": round(cores * (1.0 - util), 3),
        "mem_total_mb": round(vm.total / (1024 * 1024), 1),
        "mem_available_mb": round(vm.available / (1024 * 1024), 1),
    }


def _build_heartbeat(containers: list[ContainerReport]) -> Heartbeat:
    return Heartbeat(
        node_id=NODE_ID,
        advertise_host=settings.advertise_host,
        advertise_port=ADVERTISE_PORT,
        containers=containers,
        **_collect_metrics(),
    )


class HeartbeatLoop:
    def __init__(self, docker: DockerManager) -> None:
        self.docker = docker
        self._client = httpx.AsyncClient(timeout=5.0)
        self._task: asyncio.Task | None = None
        self._stopping = False
        self.interval = settings.heartbeat_interval_s

    async def start(self) -> None:
        psutil.cpu_percent(interval=None)  # prime the counter
        await self._register_with_retry()
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

    async def _register_with_retry(self) -> None:
        containers = await asyncio.to_thread(self.docker.report)
        hb = _build_heartbeat(containers)
        while not self._stopping:
            try:
                resp = await self._client.post(
                    f"{settings.control_plane_url}/register", json=hb.model_dump())
                resp.raise_for_status()
                self.interval = resp.json().get("heartbeat_interval_s", self.interval)
                log.info("registered %s with control plane at %s",
                         NODE_ID, settings.control_plane_url)
                return
            except Exception as exc:  # noqa: BLE001
                log.warning("register failed (%s); retrying in 2s", exc)
                await asyncio.sleep(2.0)

    async def _run(self) -> None:
        while not self._stopping:
            try:
                containers = await asyncio.to_thread(self.docker.report)
                hb = _build_heartbeat(containers)
                await self._client.post(
                    f"{settings.control_plane_url}/heartbeat", json=hb.model_dump())
            except Exception as exc:  # noqa: BLE001
                log.warning("heartbeat failed: %s", exc)
            await asyncio.sleep(self.interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.docker = DockerManager()
    app.state.hb = HeartbeatLoop(app.state.docker)
    await app.state.hb.start()
    log.info("worker agent %s up on %s:%s (advertising %s:%s)",
             NODE_ID, settings.host, settings.port, settings.advertise_host, ADVERTISE_PORT)
    yield
    await app.state.hb.stop()


app = FastAPI(title=f"Mini Cloud — Worker {NODE_ID}", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Command API (called by the control-plane reconcile loop)
# --------------------------------------------------------------------------- #
@app.post("/containers", response_model=StartContainerResult)
async def start_container(cmd: StartContainerCommand) -> StartContainerResult:
    docker: DockerManager = app.state.docker
    try:
        container_id, host_port = await asyncio.to_thread(docker.start, cmd)
    except Exception as exc:  # noqa: BLE001
        log.exception("failed to start %s", cmd.name)
        raise HTTPException(500, f"start failed: {exc}") from exc
    return StartContainerResult(
        replica_id=cmd.replica_id,
        container_id=container_id,
        host_port=host_port,
        status=ReplicaStatus.RUNNING,
    )


@app.post("/containers/stop")
async def stop_container(cmd: StopContainerCommand) -> dict:
    docker: DockerManager = app.state.docker
    await asyncio.to_thread(docker.stop, cmd.replica_id, cmd.container_id)
    return {"stopped": cmd.replica_id}


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "node_id": NODE_ID}
