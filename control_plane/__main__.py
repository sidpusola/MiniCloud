"""Run the control plane:  python -m control_plane"""
from __future__ import annotations

import uvicorn

from common.config import ControlPlaneSettings

if __name__ == "__main__":
    settings = ControlPlaneSettings()
    uvicorn.run("control_plane.app:app", host=settings.host, port=settings.port, log_level="info")
