"""Run the reverse proxy:  python -m proxy"""
from __future__ import annotations

import uvicorn

from common.config import ProxySettings

if __name__ == "__main__":
    settings = ProxySettings()
    uvicorn.run("proxy.proxy:app", host=settings.host, port=settings.port, log_level="info")
