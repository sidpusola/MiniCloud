"""Thin wrapper over the Docker SDK for the worker agent.

Owns the container lifecycle on one machine: pull image, run with CPU/memory
limits and a published port, stop/remove, and report status back. Every
container this platform creates is labelled `mc.managed=true` so the agent can
tell its own containers apart from anything else on the host.

All methods are blocking (the Docker SDK is sync); the agent calls them from a
thread via asyncio.to_thread so the event loop stays responsive.
"""
from __future__ import annotations

import logging

import docker
from docker.errors import ImageNotFound, NotFound

from common.schemas import ContainerReport, ReplicaStatus, StartContainerCommand

log = logging.getLogger("mc.worker.docker")

LABEL_MANAGED = "mc.managed"
LABEL_REPLICA = "mc.replica"
LABEL_DEPLOYMENT = "mc.deployment"


class DockerManager:
    def __init__(self) -> None:
        # from_env() honours DOCKER_HOST etc.; on Windows it talks to the
        # Docker Desktop engine over the default named pipe.
        self.client = docker.from_env()
        self.client.ping()
        log.info("connected to Docker engine")

    # ------------------------------------------------------------------ #
    def start(self, cmd: StartContainerCommand) -> tuple[str, int]:
        """Pull (if needed) and run one container. Returns (container_id, host_port)."""
        self._ensure_image(cmd.image)
        container = self.client.containers.run(
            cmd.image,
            name=cmd.name,
            detach=True,
            environment=cmd.env,
            # Publish the app port on an ephemeral host port; Docker picks it,
            # we read it back below. Binds 0.0.0.0 so other machines can reach it.
            ports={f"{cmd.container_port}/tcp": None},
            nano_cpus=int(cmd.cpu_req * 1_000_000_000),
            mem_limit=f"{int(cmd.mem_req_mb)}m",
            # The control plane owns restart/reschedule decisions, not Docker.
            restart_policy={"Name": "no"},
            labels={
                LABEL_MANAGED: "true",
                LABEL_REPLICA: cmd.replica_id,
                LABEL_DEPLOYMENT: cmd.deployment_id,
            },
        )
        container.reload()
        host_port = self._read_host_port(container, cmd.container_port)
        log.info("started %s (%s) -> host port %s", cmd.name, container.short_id, host_port)
        return container.id, host_port

    def stop(self, replica_id: str, container_id: str | None) -> None:
        container = None
        if container_id:
            try:
                container = self.client.containers.get(container_id)
            except NotFound:
                container = None
        if container is None:
            # Fall back to the replica label (id may be stale/unknown).
            matches = self.client.containers.list(
                all=True, filters={"label": f"{LABEL_REPLICA}={replica_id}"}
            )
            container = matches[0] if matches else None
        if container is None:
            return
        try:
            container.remove(force=True)
            log.info("stopped/removed container for replica %s", replica_id)
        except NotFound:
            pass

    def report(self) -> list[ContainerReport]:
        """Status of every mc-managed container on this host."""
        reports: list[ContainerReport] = []
        for c in self.client.containers.list(all=True, filters={"label": f"{LABEL_MANAGED}=true"}):
            replica_id = c.labels.get(LABEL_REPLICA)
            if not replica_id:
                continue
            status = c.status  # created / running / exited / dead ...
            reports.append(ContainerReport(
                replica_id=replica_id,
                container_id=c.id,
                docker_status=status,
                healthy=self._is_healthy(c, status),
                host_port=self._safe_host_port(c),
            ))
        return reports

    # ------------------------------------------------------------------ #
    def _ensure_image(self, image: str) -> None:
        try:
            self.client.images.get(image)
        except ImageNotFound:
            log.info("pulling image %s ...", image)
            self.client.images.pull(image)

    @staticmethod
    def _read_host_port(container, container_port: int) -> int:
        ports = container.attrs["NetworkSettings"]["Ports"]
        binding = ports.get(f"{container_port}/tcp")
        if not binding:
            raise RuntimeError(f"container {container.short_id} published no host port")
        return int(binding[0]["HostPort"])

    def _safe_host_port(self, container) -> int | None:
        try:
            ports = container.attrs["NetworkSettings"]["Ports"] or {}
            for _cp, binding in ports.items():
                if binding:
                    return int(binding[0]["HostPort"])
        except Exception:  # noqa: BLE001
            pass
        return None

    @staticmethod
    def _is_healthy(container, status: str) -> bool:
        if status != "running":
            return False
        health = container.attrs.get("State", {}).get("Health", {})
        if health:  # container defines a HEALTHCHECK
            return health.get("Status") == "healthy"
        return True  # no healthcheck -> running is good enough
