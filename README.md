# Mini Cloud Platform

A small distributed container-orchestration system — a "mini Kubernetes." You
submit a Docker image with resource requirements and a replica count; a
resource-aware **control plane** schedules the replicas onto a cluster of
**worker** machines running Docker, a **reverse proxy** load-balances traffic
across the healthy replicas, and the whole thing self-heals when containers
crash or worker nodes die.

## Concepts demonstrated

| Concept | Where |
| --- | --- |
| Resource-aware scheduling | [control_plane/scheduler.py](control_plane/scheduler.py) — worst-fit spread over CPU/memory |
| Container lifecycle mgmt | [worker/docker_manager.py](worker/docker_manager.py) — pull / run (with `--cpus`/`--memory`) / stop |
| Heartbeat failure detection | [worker/agent.py](worker/agent.py) → [control_plane/monitor.py](control_plane/monitor.py) (`_age_out_nodes`) |
| Self-healing / restart | reconcile loop reaps FAILED replicas and reschedules |
| Node-failure rescheduling | `_reap_dead_nodes` frees stranded replicas → placed elsewhere |
| Horizontal scaling | `POST /deployments/{name}/scale` |
| Service discovery + dynamic routing | [proxy/proxy.py](proxy/proxy.py) refreshes routes from `/routes` |
| Load balancing | round-robin across healthy replicas with fail-over |

## Architecture

```
                    ┌──────────────────────────────────────────┐
   deploy/scale ───▶│              CONTROL PLANE                │
   (REST API)       │  scheduler + cluster state + reconcile    │
                    │            (self-healing loop)            │
                    └───▲───────────────┬──────────────────────┘
             heartbeats │               │ start/stop container cmds
        (cpu/mem/status)│               ▼
                 ┌──────┴─────┐   ┌──────┴─────┐   ┌────────────┐
                 │  WORKER 1  │   │  WORKER 2  │   │  WORKER N  │
                 │  agent +   │   │  agent +   │   │  agent +   │
                 │  Docker    │   │  Docker    │   │  Docker    │
                 └─────▲──────┘   └─────▲──────┘   └─────▲──────┘
                       │ published container ports       │
                    ┌──┴─────────────────────────────────┴──┐
   user traffic ───▶│           REVERSE PROXY                │
   /<app>/<path>    │  routes /<deployment> → healthy pods   │
                    └────────────────────────────────────────┘
```

- **Control plane** ([control_plane/](control_plane/)) — REST API for deployments,
  the resource-aware scheduler, authoritative cluster state, and the reconcile
  loop that detects failures and drives actual state toward desired state.
- **Worker agent** ([worker/](worker/)) — runs on every worker machine; exposes a
  command API (start/stop container) and heartbeats CPU/memory/container status.
- **Reverse proxy** ([proxy/](proxy/)) — pulls the live routing table and
  load-balances inbound traffic across each deployment's healthy replicas.
- **Common** ([common/](common/)) — shared wire schemas and env-based config.

Communication is plain HTTP/JSON. The control plane **pushes** commands to
workers, workers **push** heartbeats to the control plane, and the proxy
**pulls** routes — so on a real network each worker needs its command port
(`8001`) reachable from the control plane, and its published container ports
reachable from the proxy.

## Requirements

- Python 3.10+
- Docker installed and running **on each worker machine** (Docker Desktop on
  Windows/Mac, Docker Engine on Linux). The control plane and proxy do **not**
  need Docker.

## Install

```bash
python -m venv .venv
# Windows:  .venv\Scripts\activate     Linux/Mac:  source .venv/bin/activate
pip install -r requirements.txt
```

Run every command below from the repo root so the packages resolve.

## Quick start (single machine)

Four terminals, all defaults (workers talk to `127.0.0.1:8000`):

```bash
# 1. control plane
python -m control_plane

# 2. worker #1
MC_WORKER_NODE_ID=w1 MC_WORKER_PORT=8001 python -m worker

# 3. worker #2  (different port + node id on the same host)
MC_WORKER_NODE_ID=w2 MC_WORKER_PORT=8002 python -m worker

# 4. reverse proxy
python -m proxy
```

On Windows PowerShell, set env vars first, e.g.:
```powershell
$env:MC_WORKER_NODE_ID="w2"; $env:MC_WORKER_PORT="8002"; python -m worker
```

Deploy an app (3 replicas of nginx), then hit it through the proxy:

```bash
curl -X POST http://localhost:8000/deployments \
  -H "content-type: application/json" \
  -d '{"name":"web","image":"nginx:alpine","replicas":3,"cpu_req":0.5,"mem_req_mb":128,"container_port":80}'

curl http://localhost:8000/deployments/web   # watch replicas become "running"
curl http://localhost:8080/web/               # load-balanced across replicas
```

Scale, then self-heal:

```bash
# scale to 5
curl -X POST http://localhost:8000/deployments/web/scale \
  -H "content-type: application/json" -d '{"replicas":5}'

# kill a container by hand and watch the reconcile loop replace it:
docker ps --filter "label=mc.managed=true"
docker rm -f <one-container-id>
curl http://localhost:8000/deployments/web    # a new replica is scheduled back
```

Kill a whole worker (Ctrl-C its process): after `MC_CP_DEAD_AFTER_S` the node
goes `DEAD` and its replicas are rescheduled onto the survivors.

## Multi-machine setup

Say control plane on `10.0.0.5`, workers on `10.0.0.11` and `10.0.0.12`, proxy
on `10.0.0.5`. Clone the repo on each machine, `pip install -r requirements.txt`.

**Control plane (10.0.0.5):**
```bash
python -m control_plane            # listens on 0.0.0.0:8000
```

**Each worker** — set where the control plane is and this worker's own IP:
```bash
# on 10.0.0.11
export MC_WORKER_CONTROL_PLANE_URL=http://10.0.0.5:8000
export MC_WORKER_ADVERTISE_HOST=10.0.0.11
export MC_WORKER_NODE_ID=worker-1
python -m worker
```
```bash
# on 10.0.0.12
export MC_WORKER_CONTROL_PLANE_URL=http://10.0.0.5:8000
export MC_WORKER_ADVERTISE_HOST=10.0.0.12
export MC_WORKER_NODE_ID=worker-2
python -m worker
```

**Proxy (10.0.0.5):**
```bash
export MC_PROXY_CONTROL_PLANE_URL=http://10.0.0.5:8000
python -m proxy
```

Open firewall ports: control plane `8000`, each worker `8001` (command API) plus
the published container-port range `30000–32000`, and proxy `8080`. See
[.env.example](.env.example) for every knob. `ADVERTISE_HOST` **must** be the IP
other machines use to reach the worker — this is the most common thing to get
wrong.

## API reference (control plane)

| Method & path | Purpose |
| --- | --- |
| `POST /deployments` | Create a deployment (`DeploymentSpec`) |
| `GET /deployments` / `GET /deployments/{name}` | List / inspect (replica status, endpoints) |
| `POST /deployments/{name}/scale` | `{"replicas": N}` |
| `DELETE /deployments/{name}` | Tear down + stop all containers |
| `GET /nodes` | Cluster nodes, resources, heartbeat age, status |
| `GET /routes` | Live routing table (consumed by the proxy) |
| `POST /register`, `POST /heartbeat` | Used by worker agents |

Proxy admin: `GET /__mc/routes`, `GET /__mc/healthz`. App traffic:
`ANY /<deployment-name>/<path>`.

## How self-healing works

The control plane's [reconcile loop](control_plane/monitor.py) runs every few
seconds and is the single source of convergence:

1. **Age out nodes** by heartbeat freshness: HEALTHY → UNREACHABLE → DEAD.
2. **Reap** replicas on DEAD nodes (rescheduled elsewhere), FAILED replicas
   (crashed/unhealthy containers), and orphan containers we no longer own.
3. **Scale** every deployment to its desired replica count, asking the
   [scheduler](control_plane/scheduler.py) to place new replicas on the nodes
   with the most free CPU/memory, and issuing start/stop commands to workers.

Because scale-up/scale-down, container-crash recovery, and node-failure
rescheduling all funnel through the same desired-vs-actual reconciliation, the
behaviour is consistent and easy to reason about.

## Tests

The trickiest logic — scheduling, failure detection, rescheduling, scaling — is
covered by an offline simulation that drives the real control-plane code with
stubbed workers (no Docker needed): [tests/test_reconcile.py](tests/test_reconcile.py).

```bash
python -m pytest -q          # or:  python tests/test_reconcile.py
```
