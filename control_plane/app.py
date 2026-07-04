"""Control-plane HTTP API.

Two audiences:
  * worker agents  -> POST /register, POST /heartbeat, and receive commands back
                      on their own API (driven by the reconcile loop);
  * clients/proxy  -> deployment CRUD, scaling, and cluster/route introspection.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from common.config import ControlPlaneSettings
from common.schemas import (
    DeploymentSpec,
    DeploymentView,
    Heartbeat,
    NodeStatus,
    NodeView,
    RegisterResponse,
    ReplicaStatus,
    RouteEntry,
    ScaleRequest,
    StopContainerCommand,
)
from control_plane.db import DeploymentStore
from control_plane.monitor import Reconciler
from control_plane.state import ClusterState

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("mc.control")

settings = ControlPlaneSettings()
state = ClusterState()
reconciler = Reconciler(state, settings)
store: DeploymentStore | None = None  # created in lifespan


@asynccontextmanager
async def lifespan(app: FastAPI):
    global store
    store = DeploymentStore(settings.database_url)
    # Rehydrate desired state; running containers are re-adopted from heartbeats.
    loaded = await asyncio.to_thread(store.load_all)
    async with state.lock:
        for dep in loaded:
            state.load_deployment(dep)
    log.info("loaded %d deployment(s) from %s", len(loaded), settings.database_url)
    await reconciler.start()
    log.info("control plane up on %s:%s", settings.host, settings.port)
    yield
    await reconciler.stop()


app = FastAPI(title="Mini Cloud — Control Plane", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# Worker-facing
# --------------------------------------------------------------------------- #
@app.post("/register", response_model=RegisterResponse)
async def register(hb: Heartbeat) -> RegisterResponse:
    async with state.lock:
        node = state.apply_heartbeat(hb)
    log.info("node registered: %s (%s)", node.id, node.address)
    return RegisterResponse(node_id=node.id, heartbeat_interval_s=settings.heartbeat_interval_s)


@app.post("/heartbeat")
async def heartbeat(hb: Heartbeat) -> dict:
    async with state.lock:
        state.apply_heartbeat(hb)
    return {"ok": True}


# --------------------------------------------------------------------------- #
# Deployment API
# --------------------------------------------------------------------------- #
@app.post("/deployments", response_model=DeploymentView, status_code=201)
async def create_deployment(spec: DeploymentSpec) -> DeploymentView:
    async with state.lock:
        if state.deployment_by_name(spec.name) is not None:
            raise HTTPException(409, f"deployment '{spec.name}' already exists")
        dep = state.create_deployment(spec)
        view = state.deployment_view(dep)
    await asyncio.to_thread(store.save, dep)
    log.info("created deployment %s (%s x%d)", spec.name, spec.image, spec.replicas)
    return view


@app.get("/deployments", response_model=list[DeploymentView])
async def list_deployments() -> list[DeploymentView]:
    async with state.lock:
        return [state.deployment_view(d) for d in state.deployments.values()]


@app.get("/deployments/{name}", response_model=DeploymentView)
async def get_deployment(name: str) -> DeploymentView:
    async with state.lock:
        dep = state.deployment_by_name(name)
        if dep is None:
            raise HTTPException(404, f"no deployment '{name}'")
        return state.deployment_view(dep)


@app.post("/deployments/{name}/scale", response_model=DeploymentView)
async def scale_deployment(name: str, req: ScaleRequest) -> DeploymentView:
    async with state.lock:
        dep = state.deployment_by_name(name)
        if dep is None:
            raise HTTPException(404, f"no deployment '{name}'")
        dep.desired_replicas = req.replicas
        dep_id = dep.id
        view = state.deployment_view(dep)
    await asyncio.to_thread(store.update_desired, dep_id, req.replicas)
    log.info("scaled %s -> %d replicas", name, req.replicas)
    return view


@app.delete("/deployments/{name}")
async def delete_deployment(name: str) -> dict:
    stops: list[tuple[str, StopContainerCommand]] = []
    async with state.lock:
        dep = state.deployment_by_name(name)
        if dep is None:
            raise HTTPException(404, f"no deployment '{name}'")
        for replica in state.replicas_of(dep.id):
            node = state.nodes.get(replica.node_id) if replica.node_id else None
            if node is not None and replica.container_id:
                stops.append((node.base_url, StopContainerCommand(
                    replica_id=replica.id, container_id=replica.container_id)))
            state.remove_replica(replica)
        dep_id = dep.id
        state.deployments.pop(dep_id, None)
    await asyncio.to_thread(store.delete, dep_id)
    await reconciler.send([], stops)
    log.info("deleted deployment %s", name)
    return {"deleted": name}


# --------------------------------------------------------------------------- #
# Introspection / routing
# --------------------------------------------------------------------------- #
@app.get("/nodes", response_model=list[NodeView])
async def list_nodes() -> list[NodeView]:
    async with state.lock:
        return [state.node_view(n) for n in state.nodes.values()]


@app.get("/routes", response_model=list[RouteEntry])
async def routes() -> list[RouteEntry]:
    """Routing table the reverse proxy consumes: deployment name -> healthy
    replica endpoints. Only RUNNING replicas on HEALTHY nodes are advertised."""
    async with state.lock:
        out: list[RouteEntry] = []
        for dep in state.deployments.values():
            endpoints = []
            for r in state.replicas_of(dep.id):
                node = state.nodes.get(r.node_id) if r.node_id else None
                if (
                    r.status == ReplicaStatus.RUNNING
                    and r.host_port
                    and node is not None
                    and node.status == NodeStatus.HEALTHY
                ):
                    endpoints.append(f"{node.advertise_host}:{r.host_port}")
            out.append(RouteEntry(name=dep.name, endpoints=endpoints))
        return out


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/")
async def root() -> dict:
    async with state.lock:
        return {
            "service": "mini-cloud-control-plane",
            "nodes": len(state.nodes),
            "deployments": len(state.deployments),
            "replicas": len(state.replicas),
        }
