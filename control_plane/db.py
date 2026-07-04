"""Persistence for the control plane's desired state.

Only *deployments* are stored — they are the user's declared intent and must
survive a control-plane restart. Nodes and replicas are observed state, rebuilt
from worker heartbeats after a restart (see container adoption in state.py), so
they are deliberately NOT persisted.

SQLAlchemy 2.0, SQLite by default. The engine is sync; the async API layer calls
these helpers via asyncio.to_thread so the event loop never blocks on I/O.
"""
from __future__ import annotations

import json

from sqlalchemy import String, Integer, Float, Text, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from control_plane.state import Deployment


class Base(DeclarativeBase):
    pass


class DeploymentRow(Base):
    __tablename__ = "deployments"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    image: Mapped[str] = mapped_column(String(255))
    desired_replicas: Mapped[int] = mapped_column(Integer)
    cpu_req: Mapped[float] = mapped_column(Float)
    mem_req_mb: Mapped[float] = mapped_column(Float)
    container_port: Mapped[int] = mapped_column(Integer)
    env_json: Mapped[str] = mapped_column(Text, default="{}")

    def to_domain(self) -> Deployment:
        return Deployment(
            id=self.id,
            name=self.name,
            image=self.image,
            desired_replicas=self.desired_replicas,
            cpu_req=self.cpu_req,
            mem_req_mb=self.mem_req_mb,
            container_port=self.container_port,
            env=json.loads(self.env_json or "{}"),
        )

    @classmethod
    def from_domain(cls, dep: Deployment) -> "DeploymentRow":
        return cls(
            id=dep.id,
            name=dep.name,
            image=dep.image,
            desired_replicas=dep.desired_replicas,
            cpu_req=dep.cpu_req,
            mem_req_mb=dep.mem_req_mb,
            container_port=dep.container_port,
            env_json=json.dumps(dep.env),
        )


class DeploymentStore:
    """Blocking repository for deployment rows. Wrap calls in asyncio.to_thread."""

    def __init__(self, database_url: str) -> None:
        # check_same_thread=False: SQLAlchemy pools connections across the
        # threadpool threads to_thread hands us.
        connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
        self.engine = create_engine(database_url, connect_args=connect_args, future=True)
        self._Session: sessionmaker[Session] = sessionmaker(self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)

    def load_all(self) -> list[Deployment]:
        with self._Session() as s:
            rows = s.scalars(select(DeploymentRow)).all()
            return [r.to_domain() for r in rows]

    def save(self, dep: Deployment) -> None:
        with self._Session() as s:
            s.merge(DeploymentRow.from_domain(dep))  # insert-or-update
            s.commit()

    def update_desired(self, dep_id: str, replicas: int) -> None:
        with self._Session() as s:
            row = s.get(DeploymentRow, dep_id)
            if row is not None:
                row.desired_replicas = replicas
                s.commit()

    def delete(self, dep_id: str) -> None:
        with self._Session() as s:
            row = s.get(DeploymentRow, dep_id)
            if row is not None:
                s.delete(row)
                s.commit()
