"""Environment-driven configuration for each role.

Every value has a sane default so a single-host demo works with zero config,
while a real multi-machine deployment overrides a handful of env vars.
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class ControlPlaneSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MC_CP_", env_file=".env", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8000

    # A node is considered UNREACHABLE after this many seconds without a
    # heartbeat, and DEAD (triggering rescheduling) after the dead window.
    heartbeat_interval_s: float = 3.0
    unreachable_after_s: float = 9.0
    dead_after_s: float = 20.0

    # How often the reconcile/self-heal loop runs.
    reconcile_interval_s: float = 4.0


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MC_WORKER_", env_file=".env", extra="ignore")

    # Where the control plane lives (must be set on real multi-machine setups).
    control_plane_url: str = "http://127.0.0.1:8000"

    # This agent's own command API.
    host: str = "0.0.0.0"
    port: int = 8001

    # The address the control plane and proxy should use to reach THIS worker.
    # On a real network set this to the worker's LAN/public IP.
    advertise_host: str = "127.0.0.1"
    advertise_port: int | None = None      # defaults to `port` if unset

    heartbeat_interval_s: float = 3.0

    # Range of host ports the agent may map published container ports onto.
    port_range_start: int = 30000
    port_range_end: int = 32000

    # Optional stable node id (otherwise derived from hostname).
    node_id: str | None = None


class ProxySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MC_PROXY_", env_file=".env", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8080
    control_plane_url: str = "http://127.0.0.1:8000"
    # How often the proxy refreshes its routing table from the control plane.
    refresh_interval_s: float = 3.0
    # Per-request upstream timeout.
    upstream_timeout_s: float = 30.0
