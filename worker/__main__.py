"""Run a worker agent:  python -m worker"""
from __future__ import annotations

import uvicorn

from common.config import WorkerSettings

if __name__ == "__main__":
    settings = WorkerSettings()
    uvicorn.run("worker.agent:app", host=settings.host, port=settings.port, log_level="info")
