"""The reconcile loop: failure detection, self-healing and rescheduling.

Runs on a fixed interval and drives observed cluster state toward desired state:

  * ages out nodes that stopped heartbeating (HEALTHY -> UNREACHABLE -> DEAD),
  * releases replicas stranded on DEAD nodes,
  * reaps FAILED replicas (crashed / unhealthy containers),
  * garbage-collects orphan containers a worker still runs but we no longer own,
  * scales every deployment up/down to its desired replica count, issuing
    start/stop commands to the relevant worker agents.

All state mutation happens under the store lock; all network I/O to workers
happens outside it so a slow/dead worker never blocks the whole loop.
"""
from __future__ import annotations

import asyncio
import logging
import time

import httpx

from common.config import ControlPlaneSettings
from common.schemas import (
    DeploymentStatus,
    NodeStatus,
    ReplicaStatus,
    StartContainerCommand,
    StartContainerResult,
    StopContainerCommand,
)
from control_plane.scheduler import choose_node
from control_plane.state import ClusterState, Deployment, Replica

log = logging.getLogger("mc.control.reconcile")

# Actions planned under the lock, executed over the network afterwards.
StartAction = tuple[str, StartContainerCommand]      # (worker base_url, command)
StopAction = tuple[str, StopContainerCommand]        # (worker base_url, command)


class Reconciler:
    def __init__(self, state: ClusterState, settings: ControlPlaneSettings) -> None:
        self.state = state
        self.settings = settings
        self._client = httpx.AsyncClient(timeout=10.0)
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._started_at = time.time()

    def _in_startup_grace(self) -> bool:
        """During the grace window we hold off on placing *new* replicas so that
        workers surviving a control-plane restart can re-report (and have their
        containers adopted) before we conclude anything is missing."""
        return (time.time() - self._started_at) < self.settings.startup_grace_s

    async def start(self) -> None:
        self._started_at = time.time()  # grace window counts from loop start
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
                await self.reconcile_once()
            except Exception:  # noqa: BLE001 - loop must never die
                log.exception("reconcile cycle failed")
            await asyncio.sleep(self.settings.reconcile_interval_s)

    # ------------------------------------------------------------------ #
    async def reconcile_once(self) -> None:
        starts: list[tuple[str, StartContainerCommand]] = []
        stops: list[StopAction] = []

        async with self.state.lock:
            self._age_out_nodes()
            stops += self._reap_dead_nodes()
            stops += self._reap_failed_replicas()
            stops += self._reap_orphans()
            plan_starts, plan_stops = self._scale_deployments()
            starts += plan_starts
            stops += plan_stops
            self._update_deployment_statuses()

        await self._dispatch(starts, stops)

    # ---- failure detection -------------------------------------------- #
    def _age_out_nodes(self) -> None:
        now = time.time()
        for node in self.state.nodes.values():
            age = now - node.last_heartbeat
            if age > self.settings.dead_after_s:
                node.status = NodeStatus.DEAD
            elif age > self.settings.unreachable_after_s:
                node.status = NodeStatus.UNREACHABLE
            else:
                node.status = NodeStatus.HEALTHY

    def _reap_dead_nodes(self) -> list[StopAction]:
        """Free every replica on a DEAD node so deployments fall below desired
        and get rescheduled elsewhere. No stop command — the node is gone."""
        for node in list(self.state.nodes.values()):
            if node.status != NodeStatus.DEAD:
                continue
            stranded = self.state.replicas_on(node.id)
            if stranded:
                log.warning("node %s DEAD, rescheduling %d replica(s)", node.id, len(stranded))
            for replica in stranded:
                self.state.remove_replica(replica)
        return []

    def _reap_failed_replicas(self) -> list[StopAction]:
        """Remove crashed/unhealthy replicas (stopping their remains best-effort)."""
        stops: list[StopAction] = []
        for replica in list(self.state.replicas.values()):
            if replica.status != ReplicaStatus.FAILED:
                continue
            node = self.state.nodes.get(replica.node_id) if replica.node_id else None
            if node is not None and node.status == NodeStatus.HEALTHY and replica.container_id:
                stops.append(
                    (node.base_url, StopContainerCommand(
                        replica_id=replica.id, container_id=replica.container_id))
                )
            log.info("replica %s FAILED, reaping for reschedule", replica.id)
            self.state.remove_replica(replica)
        return stops

    def _reap_orphans(self) -> list[StopAction]:
        """Stop containers a worker still runs for replicas we no longer own
        (e.g. a node came back from the dead carrying stale containers)."""
        stops: list[StopAction] = []
        for node in self.state.nodes.values():
            if node.status != NodeStatus.HEALTHY:
                continue
            for rid in node.reported_replica_ids:
                if rid not in self.state.replicas:
                    stops.append((node.base_url, StopContainerCommand(replica_id=rid)))
        return stops

    # ---- desired-state convergence ------------------------------------ #
    def _active_replicas(self, dep: Deployment) -> list[Replica]:
        out = []
        for r in self.state.replicas_of(dep.id):
            node = self.state.nodes.get(r.node_id) if r.node_id else None
            if (
                r.status in (ReplicaStatus.PENDING, ReplicaStatus.RUNNING)
                and node is not None
                and node.status == NodeStatus.HEALTHY
            ):
                out.append(r)
        return out

    def _scale_deployments(self) -> tuple[list[tuple[str, StartContainerCommand]], list[StopAction]]:
        starts: list[tuple[str, StartContainerCommand]] = []
        stops: list[StopAction] = []
        in_grace = self._in_startup_grace()

        for dep in self.state.deployments.values():
            active = self._active_replicas(dep)
            diff = dep.desired_replicas - len(active)

            if diff > 0 and not in_grace:  # scale up (suppressed during startup grace)
                for _ in range(diff):
                    node = choose_node(self.state, dep.cpu_req, dep.mem_req_mb)
                    if node is None:
                        log.warning("no capacity to place replica for %s", dep.name)
                        break
                    replica = self.state.add_replica(dep, node)
                    starts.append((
                        node.base_url,
                        StartContainerCommand(
                            replica_id=replica.id,
                            deployment_id=dep.id,
                            image=dep.image,
                            container_port=dep.container_port,
                            cpu_req=dep.cpu_req,
                            mem_req_mb=dep.mem_req_mb,
                            env=dep.env,
                            name=f"mc_{dep.name}_{replica.id}",
                        ),
                    ))

            elif diff < 0:  # scale down; drop PENDING first, then newest
                extras = sorted(
                    active,
                    key=lambda r: (r.status != ReplicaStatus.PENDING, r.created_at),
                    reverse=True,
                )[: -diff]
                for replica in extras:
                    node = self.state.nodes.get(replica.node_id)
                    if node is not None and replica.container_id:
                        stops.append((node.base_url, StopContainerCommand(
                            replica_id=replica.id, container_id=replica.container_id)))
                    self.state.remove_replica(replica)

        return starts, stops

    def _update_deployment_statuses(self) -> None:
        for dep in self.state.deployments.values():
            available = sum(
                1
                for r in self.state.replicas_of(dep.id)
                if r.status == ReplicaStatus.RUNNING
                and r.node_id in self.state.nodes
                and self.state.nodes[r.node_id].status == NodeStatus.HEALTHY
            )
            if dep.desired_replicas == 0 or available >= dep.desired_replicas:
                dep.status = DeploymentStatus.AVAILABLE
            elif available > 0:
                dep.status = DeploymentStatus.DEGRADED
            else:
                dep.status = DeploymentStatus.PROGRESSING

    # ---- network phase ------------------------------------------------- #
    async def send(self, starts: list[tuple[str, StartContainerCommand]],
                   stops: list[StopAction]) -> None:
        """Public helper so the API (e.g. delete deployment) can fire commands."""
        await self._dispatch(starts, stops)

    async def _dispatch(self, starts: list[tuple[str, StartContainerCommand]],
                        stops: list[StopAction]) -> None:
        await asyncio.gather(
            *(self._send_start(url, cmd) for url, cmd in starts),
            *(self._send_stop(url, cmd) for url, cmd in stops),
        )

    async def _send_start(self, base_url: str, cmd: StartContainerCommand) -> None:
        try:
            resp = await self._client.post(f"{base_url}/containers", json=cmd.model_dump())
            resp.raise_for_status()
            result = StartContainerResult.model_validate(resp.json())
        except Exception as exc:  # noqa: BLE001
            log.warning("start of %s on %s failed: %s", cmd.replica_id, base_url, exc)
            async with self.state.lock:
                replica = self.state.replicas.get(cmd.replica_id)
                if replica is not None:
                    replica.status = ReplicaStatus.FAILED
            return

        async with self.state.lock:
            replica = self.state.replicas.get(cmd.replica_id)
            if replica is not None:
                replica.container_id = result.container_id
                replica.host_port = result.host_port
                replica.status = result.status

    async def _send_stop(self, base_url: str, cmd: StopContainerCommand) -> None:
        try:
            await self._client.post(f"{base_url}/containers/stop", json=cmd.model_dump())
        except Exception as exc:  # noqa: BLE001
            log.debug("stop of %s on %s failed (ignored): %s", cmd.replica_id, base_url, exc)
