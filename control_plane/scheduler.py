"""Resource-aware placement.

Given a per-replica CPU/memory requirement and the current set of nodes, pick a
target node. The strategy is *worst-fit / spread*: among nodes that can fit the
replica, choose the one with the most free CPU (memory breaks ties). Spreading
replicas across nodes maximises fault tolerance — the property we care about for
self-healing — versus tightly bin-packing onto one node.
"""
from __future__ import annotations

from typing import Optional

from common.schemas import NodeStatus
from control_plane.state import ClusterState, Node


def _fits(node: Node, cpu_req: float, mem_req_mb: float) -> bool:
    return (
        node.status == NodeStatus.HEALTHY
        and node.schedulable_cpu() >= cpu_req
        and node.schedulable_mem_mb() >= mem_req_mb
    )


def choose_node(
    state: ClusterState,
    cpu_req: float,
    mem_req_mb: float,
    *,
    avoid_node_ids: Optional[set[str]] = None,
) -> Optional[Node]:
    """Return the best node for a replica, or None if the cluster is full.

    `avoid_node_ids` lets the reconciler prefer *not* to re-place a failed
    replica back onto the node it just failed on (spreads risk), while still
    falling back to those nodes if nothing else can fit.
    """
    avoid = avoid_node_ids or set()

    candidates = [n for n in state.nodes.values() if _fits(n, cpu_req, mem_req_mb)]
    if not candidates:
        return None

    preferred = [n for n in candidates if n.id not in avoid]
    pool = preferred or candidates

    # Worst-fit: most free CPU first, then most free memory.
    pool.sort(key=lambda n: (n.schedulable_cpu(), n.schedulable_mem_mb()), reverse=True)
    return pool[0]
