"""Authoritative in-memory cluster state for the control plane.

Everything the control plane knows — nodes, deployments and their replicas —
lives here behind a single asyncio lock. The reconcile loop and the HTTP API
are the only writers, and both go through this store so invariants (resource
reservations, replica<->node links) stay consistent.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from common.schemas import (
    DeploymentSpec,
    DeploymentStatus,
    DeploymentView,
    NodeStatus,
    NodeView,
    ReplicaStatus,
    ReplicaView,
)


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@dataclass
class Node:
    id: str
    advertise_host: str
    advertise_port: int
    cpu_total: float = 0.0
    cpu_available: float = 0.0          # last reported free cores
    mem_total_mb: float = 0.0
    mem_available_mb: float = 0.0       # last reported free MB
    last_heartbeat: float = field(default_factory=time.time)
    status: NodeStatus = NodeStatus.HEALTHY
    # Resources committed by the scheduler (sum of this node's replica requests).
    reserved_cpu: float = 0.0
    reserved_mem_mb: float = 0.0
    # replica_ids the worker reported running on its last heartbeat.
    reported_replica_ids: set[str] = field(default_factory=set)

    @property
    def address(self) -> str:
        return f"{self.advertise_host}:{self.advertise_port}"

    @property
    def base_url(self) -> str:
        return f"http://{self.advertise_host}:{self.advertise_port}"

    def schedulable_cpu(self) -> float:
        return max(0.0, self.cpu_total - self.reserved_cpu)

    def schedulable_mem_mb(self) -> float:
        return max(0.0, self.mem_total_mb - self.reserved_mem_mb)


@dataclass
class Replica:
    id: str
    deployment_id: str
    cpu_req: float
    mem_req_mb: float
    node_id: Optional[str] = None
    container_id: Optional[str] = None
    host_port: Optional[int] = None
    status: ReplicaStatus = ReplicaStatus.PENDING
    created_at: float = field(default_factory=time.time)
    # Last time the worker's Docker daemon reported this container present.
    # Used to detect containers that vanish (removed, not merely exited).
    last_seen: float = field(default_factory=time.time)


@dataclass
class Deployment:
    id: str
    name: str
    image: str
    desired_replicas: int
    cpu_req: float
    mem_req_mb: float
    container_port: int
    env: dict[str, str]
    status: DeploymentStatus = DeploymentStatus.PENDING


class ClusterState:
    def __init__(self) -> None:
        import asyncio

        self.lock = asyncio.Lock()
        self.nodes: dict[str, Node] = {}
        self.deployments: dict[str, Deployment] = {}
        self.replicas: dict[str, Replica] = {}

    # ---- lookups (call under lock) ------------------------------------- #
    def replicas_of(self, deployment_id: str) -> list[Replica]:
        return [r for r in self.replicas.values() if r.deployment_id == deployment_id]

    def replicas_on(self, node_id: str) -> list[Replica]:
        return [r for r in self.replicas.values() if r.node_id == node_id]

    def deployment_by_name(self, name: str) -> Optional[Deployment]:
        for d in self.deployments.values():
            if d.name == name:
                return d
        return None

    # ---- node registration / heartbeat --------------------------------- #
    def upsert_node(
        self,
        node_id: str,
        advertise_host: str,
        advertise_port: int,
        cpu_total: float,
        cpu_available: float,
        mem_total_mb: float,
        mem_available_mb: float,
    ) -> Node:
        node = self.nodes.get(node_id)
        if node is None:
            node = Node(id=node_id, advertise_host=advertise_host, advertise_port=advertise_port)
            self.nodes[node_id] = node
        node.advertise_host = advertise_host
        node.advertise_port = advertise_port
        node.cpu_total = cpu_total
        node.cpu_available = cpu_available
        node.mem_total_mb = mem_total_mb
        node.mem_available_mb = mem_available_mb
        node.last_heartbeat = time.time()
        node.status = NodeStatus.HEALTHY
        return node

    def apply_heartbeat(self, hb) -> Node:
        """Fold a worker heartbeat into cluster state.

        Updates node metrics, records which replicas the worker is actually
        running, and reconciles each known replica's status against what Docker
        reports (a container that exited becomes FAILED -> triggers self-heal).
        """
        node = self.upsert_node(
            node_id=hb.node_id,
            advertise_host=hb.advertise_host,
            advertise_port=hb.advertise_port,
            cpu_total=hb.cpu_total,
            cpu_available=hb.cpu_available,
            mem_total_mb=hb.mem_total_mb,
            mem_available_mb=hb.mem_available_mb,
        )
        node.reported_replica_ids = {c.replica_id for c in hb.containers}

        for report in hb.containers:
            replica = self.replicas.get(report.replica_id)
            if replica is None:
                # Unknown replica. If its deployment still exists (e.g. the
                # control plane restarted and lost in-memory replicas), adopt
                # the running container instead of treating it as an orphan.
                replica = self._maybe_adopt(node, report)
                if replica is None:
                    continue  # true orphan; the reconcile loop will order a stop
            if report.container_id and not replica.container_id:
                replica.container_id = report.container_id
            if report.host_port and not replica.host_port:
                replica.host_port = report.host_port
            replica.last_seen = time.time()  # container is still present on the node

            # Don't resurrect a replica we intentionally stopped.
            if replica.status == ReplicaStatus.STOPPED:
                continue
            if report.docker_status == "running" and report.healthy:
                replica.status = ReplicaStatus.RUNNING
            elif report.docker_status in ("exited", "dead", "removing"):
                replica.status = ReplicaStatus.FAILED
        return node

    def _maybe_adopt(self, node: Node, report) -> Optional["Replica"]:
        """Re-attach a running container to a known deployment after a restart.

        Returns the (re)created Replica, or None if the container belongs to no
        known deployment (a genuine orphan to be stopped).
        """
        dep = self.deployments.get(report.deployment_id) if report.deployment_id else None
        if dep is None:
            return None
        replica = Replica(
            id=report.replica_id,               # preserve id so it keeps matching
            deployment_id=dep.id,
            cpu_req=dep.cpu_req,
            mem_req_mb=dep.mem_req_mb,
            node_id=node.id,
            container_id=report.container_id,
            host_port=report.host_port,
            status=ReplicaStatus.PENDING,
        )
        self.replicas[replica.id] = replica
        self.reserve(node, replica)
        return replica

    # ---- reservation accounting ---------------------------------------- #
    def reserve(self, node: Node, replica: Replica) -> None:
        node.reserved_cpu += replica.cpu_req
        node.reserved_mem_mb += replica.mem_req_mb

    def release(self, replica: Replica) -> None:
        node = self.nodes.get(replica.node_id) if replica.node_id else None
        if node is not None:
            node.reserved_cpu = max(0.0, node.reserved_cpu - replica.cpu_req)
            node.reserved_mem_mb = max(0.0, node.reserved_mem_mb - replica.mem_req_mb)

    def add_replica(self, deployment: Deployment, node: Node) -> Replica:
        replica = Replica(
            id=_new_id("rep"),
            deployment_id=deployment.id,
            cpu_req=deployment.cpu_req,
            mem_req_mb=deployment.mem_req_mb,
            node_id=node.id,
        )
        self.replicas[replica.id] = replica
        self.reserve(node, replica)
        return replica

    def remove_replica(self, replica: Replica) -> None:
        self.release(replica)
        self.replicas.pop(replica.id, None)

    # ---- deployments --------------------------------------------------- #
    def create_deployment(self, spec: DeploymentSpec) -> Deployment:
        dep = Deployment(
            id=_new_id("dep"),
            name=spec.name,
            image=spec.image,
            desired_replicas=spec.replicas,
            cpu_req=spec.cpu_req,
            mem_req_mb=spec.mem_req_mb,
            container_port=spec.container_port,
            env=dict(spec.env),
        )
        self.deployments[dep.id] = dep
        return dep

    def load_deployment(self, dep: Deployment) -> None:
        """Insert a deployment rehydrated from the database at startup."""
        self.deployments[dep.id] = dep

    # ---- view builders ------------------------------------------------- #
    def _replica_view(self, r: Replica) -> ReplicaView:
        endpoint = None
        node = self.nodes.get(r.node_id) if r.node_id else None
        if node is not None and r.host_port:
            endpoint = f"{node.advertise_host}:{r.host_port}"
        return ReplicaView(
            id=r.id,
            deployment_id=r.deployment_id,
            node_id=r.node_id,
            container_id=r.container_id,
            status=r.status,
            endpoint=endpoint,
        )

    def deployment_view(self, dep: Deployment) -> DeploymentView:
        reps = self.replicas_of(dep.id)
        available = sum(
            1
            for r in reps
            if r.status == ReplicaStatus.RUNNING
            and r.node_id in self.nodes
            and self.nodes[r.node_id].status == NodeStatus.HEALTHY
        )
        return DeploymentView(
            id=dep.id,
            name=dep.name,
            image=dep.image,
            desired_replicas=dep.desired_replicas,
            available_replicas=available,
            status=dep.status,
            container_port=dep.container_port,
            replicas=[self._replica_view(r) for r in reps],
        )

    def node_view(self, node: Node) -> NodeView:
        return NodeView(
            id=node.id,
            address=node.address,
            status=node.status,
            cpu_total=node.cpu_total,
            cpu_available=node.cpu_available,
            mem_total_mb=node.mem_total_mb,
            mem_available_mb=node.mem_available_mb,
            last_heartbeat_age_s=round(time.time() - node.last_heartbeat, 2),
            replica_count=len(self.replicas_on(node.id)),
        )
