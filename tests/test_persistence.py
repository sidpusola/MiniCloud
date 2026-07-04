"""Tests for control-plane persistence and restart recovery.

Covers three things, all without Docker or live workers:
  * the SQLite deployment store round-trips (save / load / update / delete);
  * the startup grace window suppresses scheduling until workers re-report;
  * a control-plane restart adopts still-running containers instead of
    duplicating them.
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import ControlPlaneSettings
from common.schemas import (
    ContainerReport,
    DeploymentSpec,
    Heartbeat,
    ReplicaStatus,
)
from control_plane.db import DeploymentStore
from control_plane.monitor import Reconciler
from control_plane.state import ClusterState


def _hb(node_id: str, port: int, cpu: float, mem: float, containers=None) -> Heartbeat:
    return Heartbeat(
        node_id=node_id, advertise_host="10.0.0.5", advertise_port=port,
        cpu_total=cpu, cpu_available=cpu, mem_total_mb=mem, mem_available_mb=mem,
        containers=containers or [],
    )


class StubReconciler(Reconciler):
    """Simulate worker start responses instead of real HTTP calls."""

    async def _dispatch(self, starts, stops):
        async with self.state.lock:
            for _url, cmd in starts:
                r = self.state.replicas.get(cmd.replica_id)
                if r is not None:
                    r.container_id = "cid-" + cmd.replica_id
                    r.host_port = 30000 + (abs(hash(cmd.replica_id)) % 1000)
                    r.status = ReplicaStatus.RUNNING


def _tmp_db_url() -> str:
    path = os.path.join(tempfile.mkdtemp(), "mc_test.db").replace("\\", "/")
    return f"sqlite:///{path}"


# --------------------------------------------------------------------------- #
def test_deployment_store_roundtrip() -> None:
    store = DeploymentStore(_tmp_db_url())
    state = ClusterState()
    dep = state.create_deployment(DeploymentSpec(
        name="api", image="nginx:alpine", replicas=3,
        cpu_req=0.5, mem_req_mb=256, container_port=8080, env={"K": "V"}))

    store.save(dep)
    loaded = store.load_all()
    assert len(loaded) == 1
    got = loaded[0]
    assert got.id == dep.id and got.name == "api" and got.desired_replicas == 3
    assert got.env == {"K": "V"} and got.container_port == 8080

    store.update_desired(dep.id, 7)
    assert store.load_all()[0].desired_replicas == 7

    store.delete(dep.id)
    assert store.load_all() == []


def test_startup_grace_suppresses_then_resumes() -> None:
    async def scenario() -> None:
        settings = ControlPlaneSettings()
        state = ClusterState()
        rec = StubReconciler(state, settings)          # grace window is active
        rec._started_at = time.time()

        # A healthy node with capacity, and a deployment wanting 2 replicas.
        async with state.lock:
            state.apply_heartbeat(_hb("nodeA", 8001, 4, 4096))
            state.create_deployment(DeploymentSpec(
                name="web", image="nginx:alpine", replicas=2,
                cpu_req=1.0, mem_req_mb=512, container_port=80))

        await rec.reconcile_once()
        assert len(state.replicas) == 0, "grace must suppress scale-up despite free capacity"

        rec._started_at = time.time() - 999            # grace has now elapsed
        await rec.reconcile_once()
        assert len(state.replicas) == 2, "scheduling resumes once grace elapses"
        await rec._client.aclose()

    asyncio.run(scenario())


def test_restart_adopts_running_containers() -> None:
    async def scenario() -> None:
        settings = ControlPlaneSettings()
        store = DeploymentStore(_tmp_db_url())

        # --- lifetime 1: deploy 2 replicas and persist the deployment ---
        state1 = ClusterState()
        rec1 = StubReconciler(state1, settings)
        rec1._started_at = time.time() - 999           # no grace, act immediately
        async with state1.lock:
            state1.apply_heartbeat(_hb("nodeA", 8001, 4, 4096))
            dep = state1.create_deployment(DeploymentSpec(
                name="web", image="nginx:alpine", replicas=2,
                cpu_req=1.0, mem_req_mb=512, container_port=80))
        store.save(dep)
        await rec1.reconcile_once()
        placed = state1.replicas_of(dep.id)
        assert len(placed) == 2
        # What the worker's Docker daemon would report post-restart.
        running_reports = [
            ContainerReport(replica_id=r.id, deployment_id=dep.id,
                            container_id=r.container_id, docker_status="running",
                            healthy=True, host_port=r.host_port)
            for r in placed
        ]
        await rec1._client.aclose()

        # --- simulate CONTROL-PLANE RESTART: fresh state, same DB ---
        state2 = ClusterState()
        rec2 = StubReconciler(state2, settings)        # grace active again
        rec2._started_at = time.time() - 999           # but skip it for this check
        async with state2.lock:
            for d in store.load_all():
                state2.load_deployment(d)
        assert len(state2.deployments) == 1 and len(state2.replicas) == 0

        # The surviving worker heartbeats its still-running containers.
        async with state2.lock:
            state2.apply_heartbeat(_hb("nodeA", 8001, 4, 4096, containers=running_reports))
        assert len(state2.replicas) == 2, "running containers adopted, not forgotten"
        assert all(r.status == ReplicaStatus.RUNNING for r in state2.replicas.values())

        # Reconcile must NOT create duplicates for the adopted replicas.
        await rec2.reconcile_once()
        assert len(state2.replicas) == 2, "adoption prevents duplicate scheduling"
        assert state2.nodes["nodeA"].reserved_cpu == 2.0, "resources re-reserved on adoption"
        await rec2._client.aclose()

    asyncio.run(scenario())


if __name__ == "__main__":
    test_deployment_store_roundtrip()
    test_startup_grace_suppresses_then_resumes()
    test_restart_adopts_running_containers()
    print("ALL PERSISTENCE + ADOPTION ASSERTIONS PASSED")
