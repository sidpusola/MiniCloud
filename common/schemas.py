"""Wire contracts shared across the control plane, worker agents and proxy.

Every HTTP payload that crosses a machine boundary is defined here so the three
roles stay in lock-step even though they run as separate processes / hosts.
"""
from __future__ import annotations

import time
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class NodeStatus(str, Enum):
    HEALTHY = "healthy"        # heartbeat received within the grace window
    UNREACHABLE = "unreachable"  # missed a few heartbeats, not yet evicted
    DEAD = "dead"              # evicted; its replicas get rescheduled


class ReplicaStatus(str, Enum):
    PENDING = "pending"    # scheduled, container not yet confirmed running
    RUNNING = "running"    # container up and (optionally) health-checked
    FAILED = "failed"      # container exited / unhealthy
    STOPPED = "stopped"    # intentionally torn down


class DeploymentStatus(str, Enum):
    PENDING = "pending"
    PROGRESSING = "progressing"  # desired != available, reconciling
    AVAILABLE = "available"      # desired replicas running
    DEGRADED = "degraded"        # some replicas down, self-healing in progress


# --------------------------------------------------------------------------- #
# Worker -> Control plane
# --------------------------------------------------------------------------- #
class ContainerReport(BaseModel):
    """Status of one container as seen by the worker's Docker daemon."""
    replica_id: str
    deployment_id: Optional[str] = None   # from the mc.deployment label; enables adoption
    container_id: Optional[str] = None
    docker_status: str = "unknown"   # created/running/exited/dead...
    healthy: bool = False
    host_port: Optional[int] = None


class Heartbeat(BaseModel):
    """Sent by each worker agent on a fixed interval."""
    node_id: str
    # How to reach this worker's command API (control plane -> worker).
    advertise_host: str
    advertise_port: int
    # Resource snapshot.
    cpu_total: float                 # logical cores
    cpu_available: float             # cores currently free (cores * (1 - util))
    mem_total_mb: float
    mem_available_mb: float
    # What Docker is actually running right now.
    containers: list[ContainerReport] = Field(default_factory=list)
    ts: float = Field(default_factory=time.time)


class RegisterResponse(BaseModel):
    node_id: str
    accepted: bool = True
    heartbeat_interval_s: float = 3.0


# --------------------------------------------------------------------------- #
# Control plane -> Worker (command API)
# --------------------------------------------------------------------------- #
class StartContainerCommand(BaseModel):
    replica_id: str
    deployment_id: str
    image: str
    container_port: int              # port the app listens on inside the container
    cpu_req: float                   # cores to reserve (used for --cpus limit)
    mem_req_mb: float                # MB to reserve (used for --memory limit)
    env: dict[str, str] = Field(default_factory=dict)
    name: str


class StartContainerResult(BaseModel):
    replica_id: str
    container_id: str
    host_port: int                   # port on the worker host mapped to container_port
    status: ReplicaStatus


class StopContainerCommand(BaseModel):
    replica_id: str
    container_id: Optional[str] = None


# --------------------------------------------------------------------------- #
# Client -> Control plane (deployment API)
# --------------------------------------------------------------------------- #
class DeploymentSpec(BaseModel):
    name: str = Field(..., pattern=r"^[a-z0-9][a-z0-9-]{0,40}$")
    image: str
    replicas: int = Field(1, ge=1, le=100)
    cpu_req: float = Field(0.5, gt=0)          # cores per replica
    mem_req_mb: float = Field(256, gt=0)       # MB per replica
    container_port: int = Field(80, ge=1, le=65535)
    env: dict[str, str] = Field(default_factory=dict)


class ScaleRequest(BaseModel):
    replicas: int = Field(..., ge=0, le=100)


# --------------------------------------------------------------------------- #
# Control plane -> Client / Proxy (views)
# --------------------------------------------------------------------------- #
class ReplicaView(BaseModel):
    id: str
    deployment_id: str
    node_id: Optional[str]
    container_id: Optional[str]
    status: ReplicaStatus
    endpoint: Optional[str] = None   # "host:port" reachable through the worker


class DeploymentView(BaseModel):
    id: str
    name: str
    image: str
    desired_replicas: int
    available_replicas: int
    status: DeploymentStatus
    container_port: int
    replicas: list[ReplicaView] = Field(default_factory=list)


class NodeView(BaseModel):
    id: str
    address: str
    status: NodeStatus
    cpu_total: float
    cpu_available: float
    mem_total_mb: float
    mem_available_mb: float
    last_heartbeat_age_s: float
    replica_count: int


class RouteEntry(BaseModel):
    """One routable target the proxy can forward to."""
    name: str                        # deployment name -> becomes the URL prefix
    endpoints: list[str]             # healthy "host:port" backends
