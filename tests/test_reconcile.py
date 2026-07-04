"""Offline test of the control-plane orchestration + self-healing.

Drives the real ClusterState / scheduler / Reconciler, but stubs the network
phase (worker HTTP) so placement, failure detection, rescheduling and scaling
are exercised without Docker or live workers.

Run:  python -m pytest -q       (or)      python tests/test_reconcile.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.config import ControlPlaneSettings
from common.schemas import DeploymentSpec, Heartbeat, NodeStatus, ReplicaStatus
from control_plane.monitor import Reconciler
from control_plane.state import ClusterState


def _hb(node_id: str, port: str, cpu_total: float, mem_total: float) -> Heartbeat:
    return Heartbeat(
        node_id=node_id, advertise_host="10.0.0." + port[-1], advertise_port=int(port),
        cpu_total=cpu_total, cpu_available=cpu_total,
        mem_total_mb=mem_total, mem_available_mb=mem_total,
    )


class StubReconciler(Reconciler):
    """Simulate worker responses instead of issuing real HTTP calls."""

    async def _dispatch(self, starts, stops):
        async with self.state.lock:
            for _url, cmd in starts:
                r = self.state.replicas.get(cmd.replica_id)
                if r is None:
                    continue
                r.container_id = "cid-" + cmd.replica_id
                r.host_port = 30000 + (abs(hash(cmd.replica_id)) % 1000)
                r.status = ReplicaStatus.RUNNING
        self.last_stops = list(stops)


def _counts(state: ClusterState, dep_id: str):
    reps = state.replicas_of(dep_id)
    by_node: dict = {}
    for r in reps:
        by_node[r.node_id] = by_node.get(r.node_id, 0) + 1
    running = sum(1 for r in reps if r.status == ReplicaStatus.RUNNING)
    return len(reps), running, by_node


async def _scenario() -> None:
    settings = ControlPlaneSettings()
    state = ClusterState()
    rec = StubReconciler(state, settings)
    rec._started_at = time.time() - 999   # bypass startup grace; this tests scheduling

    # Two nodes, 4 cores / 4096 MB each.
    async with state.lock:
        state.apply_heartbeat(_hb("nodeA", "8001", 4, 4096))
        state.apply_heartbeat(_hb("nodeB", "8002", 4, 4096))

    # Deploy 3 replicas @ 1 core / 512 MB.
    async with state.lock:
        dep = state.create_deployment(DeploymentSpec(
            name="web", image="nginx:alpine", replicas=3,
            cpu_req=1.0, mem_req_mb=512, container_port=80))
    await rec.reconcile_once()
    total, running, by_node = _counts(state, dep.id)
    assert total == 3 and running == 3, "should place 3 running replicas"
    assert len(by_node) == 2, "worst-fit should spread across both nodes"
    assert state.nodes["nodeA"].reserved_cpu + state.nodes["nodeB"].reserved_cpu == 3.0

    # Container failure -> reaped and replaced.
    dead = state.replicas_of(dep.id)[0]
    async with state.lock:
        dead.status = ReplicaStatus.FAILED
    await rec.reconcile_once()
    _, running, _ = _counts(state, dep.id)
    assert running == 3, "failed container should self-heal back to 3"
    assert dead.id not in state.replicas, "failed replica reaped"

    # Node failure -> replicas rescheduled to survivor.
    async with state.lock:
        state.nodes["nodeB"].last_heartbeat = time.time() - 999
    await rec.reconcile_once()
    _, running, by_node = _counts(state, dep.id)
    assert state.nodes["nodeB"].status == NodeStatus.DEAD
    assert running == 3 and set(by_node) == {"nodeA"}, "rescheduled onto nodeA"

    # Scale down to 1.
    async with state.lock:
        dep.desired_replicas = 1
    await rec.reconcile_once()
    total, running, _ = _counts(state, dep.id)
    assert total == 1 and running == 1, "should scale to a single replica"

    # Scale up to 5 but only a 4-core node is alive -> capped at 4.
    async with state.lock:
        dep.desired_replicas = 5
    await rec.reconcile_once()
    _, running, _ = _counts(state, dep.id)
    assert running == 4, "1-core replicas cap at 4 on a single 4-core node"

    # nodeB recovers -> 5th replica placed.
    async with state.lock:
        state.apply_heartbeat(_hb("nodeB", "8002", 4, 4096))
    await rec.reconcile_once()
    _, running, _ = _counts(state, dep.id)
    assert running == 5, "5th replica placed once nodeB returns"

    await rec._client.aclose()


def test_orchestration_and_self_healing() -> None:
    asyncio.run(_scenario())


if __name__ == "__main__":
    asyncio.run(_scenario())
    print("ALL SIMULATION ASSERTIONS PASSED")
