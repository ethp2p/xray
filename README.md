# Xray

Real-time bandwidth usage profiler for the Ethereum Consensus Layer, with:

- per-object, per-flow, per-slot traffic attribution, e.g. attestations, aggregates, blocks, blobs, etc.
- spill-over traffic analysis, e.g. objects from slot N-1 that continue propagating in slot N

**Live dashboard**: [xray.ethp2p.dev](https://xray.ethp2p.dev)

## How it works

Xray comprises two components: the client-side **probe** and the **collector** backend.

**Xray probe:** wraps the libp2p `Host`, intercepts every `Read`/`Write` on every stream, and forwards raw byte chunks over a lightweight ingest protocol to the backend over a local socket. The probe has no Ethereum-specific logic: it sends opaque bytes.

**Xray backend:** is a standalone binary (`cmd/xray`). It accepts probe connections, reassembles gossipsub RPC frames, decodes SSZ payloads to extract slot numbers and block metadata, then aggregates traffic into 100ms time buckets per slot. It serves a REST + WebSocket API for the dashboard and persists finalized slots and source metadata to SQLite.

Here's an architecture diagram:

```
┌───────────────────────────────────────────────────────────────┐
│                     Ethereum CL client                        │
│                  (Prysm, Lighthouse, etc.)                    │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │              probe SDK (github.com/ethp2p/xray/probe)   │  │
│  │  Wraps libp2p Host, intercepts streams/connections      │  │
│  │  Forwards raw bytes via ingest protocol                 │  │
│  └──────────────────────┬──────────────────────────────────┘  │
└─────────────────────────┼─────────────────────────────────────┘
                          │ Unix socket / TCP
                          │ (ClientHello -> ServerHello -> Envelopes)
                          v
┌─────────────────────────────────────────────────────────────┐
│                   Backend (cmd/xray)                        │
│                                                             │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌────────────┐  │
│  │ Ingest   │->│ Processor │->│ Storage  │  │ HTTP/WS    │  │
│  │ listener │  │ (per-src) │  │ (SQLite) │  │ server     │  │
│  └──────────┘  └───────────┘  └──────────┘  └─────┬──────┘  │
│                                                   │         │
│  internal/gossipsub --- RPC parser                │         │
│  internal/eth ───────── SSZ decoder, slot clock   │         │
└───────────────────────────────────────────────────┼─────────┘
                                                    │
                                            ┌───────v─────────┐
                                            │  Xray Dashboard │
                                            │  (Solid.js)     │
                                            └─────────────────┘
```

## Installation

The supported install is the **full stack** on Linux: Nethermind, instrumented
Prysm, and Xray, managed as Podman Quadlets under systemd. Unit files are
fetched from the release tag; no repository clone is required.

Upgrades, rollback, and image publishing: [`infra/README.md`](infra/README.md).

### Prerequisites

| Requirement | Notes |
| --- | --- |
| Linux host with systemd | Quadlets are systemd generators |
| Podman 4.9+ | Rootful Podman; units under `/etc/containers/systemd` |
| uid/gid `1000` | All three units run as `User=1000` / `Group=1000` |
| Disk | `/data/nethermind`, `/data/.eth2`, and the Xray data dir (default `/home/ubuntu/.xray/data`) |
| Engine JWT file | Shared secret for Nethermind ↔ Prysm (`podman secret create`) |

### Prepare

```bash
REF=v0.1.0
BASE=https://raw.githubusercontent.com/ethp2p/xray/${REF}

sudo apt-get update
sudo apt-get install --yes podman

curl -fsSL "$BASE/infra/tmpfiles/xray.conf" \
  | sudo tee /etc/tmpfiles.d/xray.conf >/dev/null
sudo systemd-tmpfiles --create /etc/tmpfiles.d/xray.conf

sudo install -d -o 1000 -g 1000 -m 0750 \
  /data/nethermind /data/.eth2 /home/ubuntu/.xray/data

sudo podman pull ghcr.io/ethp2p/xray:0.1.0
sudo podman pull ghcr.io/ethp2p/xray-prysm:stable
sudo podman pull \
  docker.io/nethermind/nethermind@sha256:d915b29966286ec9ceee400c889e0b18fd4d84e7895402f3f4fa5750209c0a25

sudo podman secret create eth-jwt /path/to/jwt.hex
```

`/run/xray` holds the ingest socket (`xray.sock`). Edit the Xray `Volume=`
path in the unit before install if your data dir is not
`/home/ubuntu/.xray/data`.

### Install

```bash
REF=v0.1.0
BASE=https://raw.githubusercontent.com/ethp2p/xray/${REF}

sudo install -d -m 0755 /etc/containers/systemd
for unit in nethermind.container xray.container prysm.container; do
  curl -fsSL "$BASE/infra/quadlet/${unit}" \
    | sudo tee "/etc/containers/systemd/${unit}" >/dev/null
done

sudo env QUADLET_UNIT_DIRS=/etc/containers/systemd \
  /usr/lib/systemd/system-generators/podman-system-generator --dryrun
sudo systemctl daemon-reload
```

### Start

```bash
sudo systemctl start nethermind.service
sudo systemctl start xray.service
sudo systemctl start prysm.service
```

Prysm waits for Xray on `/run/xray/xray.sock`
(`--p2p-instrument-wait-for-attach`). Loopback ports: Xray `:9100`,
Nethermind JSON-RPC `:8545`, Prysm API `:3500`.

### Verify

```bash
systemctl --no-pager --full status \
  nethermind.service xray.service prysm.service
curl -fsS http://127.0.0.1:9100/api/sources
curl -fsS http://127.0.0.1:3500/eth/v1/node/syncing
```

Open `http://127.0.0.1:9100`. A healthy stack shows a connected Prysm source
in `/api/sources`.

### Build from source

For local development without Podman (including macOS). Needs Go 1.25, CGO,
a C compiler, SQLite headers, and Bun for the dashboard.

```bash
git clone https://github.com/ethp2p/xray.git
cd xray

install -d ./bin "$HOME/.xray/data"
go build -o ./bin/xray ./cmd/xray

cd dashboard
bun install --frozen-lockfile
bun run build
cd ..

./bin/xray \
  --ingest=/tmp/xray.sock \
  --listen=127.0.0.1:9100 \
  --data-dir="$HOME/.xray/data" \
  --static-dir=dashboard/dist
```

Point an instrumented client at the ingest socket, or use the Quadlet stack
above for a full node.

### Docker Compose

Legacy alternative that builds only the Xray backend + dashboard (no EL/CL).
Prefer Quadlets on Linux hosts.

```bash
git clone https://github.com/ethp2p/xray.git
cd xray
export XRAY_DATA_DIR="$HOME/.xray/data"
export XRAY_SOCK_DIR="$HOME/.xray/run"
export XRAY_UID="$(id -u)"
export XRAY_GID="$(id -g)"
install -d "$XRAY_DATA_DIR" "$XRAY_SOCK_DIR"
docker compose up --detach --build
```

Socket: `$XRAY_SOCK_DIR/xray.sock`.

## Quick start

### Run the backend during development

Start the backend:

```bash
go build -o xray ./cmd/xray
./xray --ingest=/tmp/xray.sock --listen=127.0.0.1:9100
```

The backend uses `github.com/mattn/go-sqlite3`, so local builds need CGO enabled
and a working C compiler.

Start the dashboard dev server:

```bash
cd dashboard && bun install && bun run dev
```

Open `http://localhost:5173`. Vite proxies API requests to the backend on `:9100`.

### Integrate with Prysm

The maintained Prysm integration is the
[`ethp2p/prysm`](https://github.com/ethp2p/prysm) `xray` branch, pinned in
the production deployment to commit
`1fcc706ce44eacd253ae3f5078995c5b3437e5fd`.

```go
import "github.com/ethp2p/xray/probe"

ih, err := probe.Wrap(h,
    probe.WithIngestAddr("/tmp/xray.sock"),
    probe.WithClientName("prysm"),
    probe.WithWaitForAttach(),
)
```

Build the fork, then pass the socket to `beacon-chain`:

```bash
git clone --branch xray https://github.com/ethp2p/prysm.git
cd prysm
go build -o ./bin/beacon-chain ./cmd/beacon-chain

./bin/beacon-chain \
  --accept-terms-of-use \
  --mainnet \
  --execution-endpoint=http://127.0.0.1:8551 \
  --jwt-secret=/path/to/jwt.hex \
  --datadir=/path/to/prysm-data \
  --p2p-instrument-socket=/path/to/xray.sock \
  --p2p-instrument-wait-for-attach
```

The fork also supports `--p2p-instrument-file` for a local protobuf trace.
`--p2p-instrument-wait-for-attach` blocks Prysm startup until Xray connects;
omit it if instrumentation must not hold up the node.

## Project structure

```
probe/                  Producer SDK (host wrapper, sinks, emitter)
*.go                    Deprecated root compat shim (re-exports probe)
api/                    Shared JSON DTOs for REST/WebSocket responses
cmd/xray/               Backend binary entrypoint
internal/decode/        Stream-decode types shared by gossipsub and processor
internal/eth/           Ethereum slot clock and SSZ extraction
internal/gossipsub/     Gossipsub RPC parser
internal/ingest/        Probe connection listener and ingest sessions
internal/processor/     Per-source aggregation and finalized slot production
internal/server/        REST/WebSocket API server
internal/sources/       Probe source registry
internal/storage/       SQLite persistence for sources and finalized slots
proto/xray/             Protobuf definitions and generated ingest messages
proto/xray/wire/        Typed length-delimited ingest protocol codec
itest/                  Integration tests (gossipsub decoding, introspector E2E)
dashboard/              Solid.js web dashboard ("Ethereum Xray")
clients/                Hand-written Rust and JavaScript producer SDKs
gen/                    Generated support code
infra/                  Podman Quadlets and production deployment runbook
```

## Probe integration

The probe wraps a `go-libp2p` host transparently:

```go
import "github.com/ethp2p/xray/probe"

host, _ := libp2p.New(...)
ih, err := probe.Wrap(host,
    probe.WithIngestAddr("/tmp/xray.sock"),
    probe.WithClientName("my-client/v1.0"),
    probe.WithWaitForAttach(),                    // block until backend connects
    probe.WithSinkFile("/var/log/xray.trace"),    // optional local trace file
)
defer ih.Close()
```

`probe.Wrap` returns a `*probe.Host` that satisfies `host.Host`. Existing code works unchanged; all stream reads/writes are intercepted and forwarded. The root import `github.com/ethp2p/xray` still re-exports this surface (including deprecated `Wiretap`) for Prysm `@1fcc706ce`.

## Configuration

Backend CLI flags (`cmd/xray`):

| Flag                 | Default          | Description                                      |
| -------------------- | ---------------- | ------------------------------------------------ |
| `--ingest`           | `/tmp/xray.sock` | Ingest listener address (Unix path or host:port) |
| `--listen`           | `127.0.0.1:9100` | HTTP listen address for REST/WS API              |
| `--data-dir`         | `~/.xray/data`   | Persistence directory for slot data              |
| `--retention-days`   | `30`             | Slot retention period in days                    |
| `--genesis-unix`     | `1606824023`     | Beacon chain genesis Unix timestamp              |
| `--seconds-per-slot` | `12`             | Beacon chain seconds per slot                    |
| `--static-dir`       | (none)           | Serve dashboard static files from this directory |

## API

### REST endpoints

| Method | Path                                                 | Description                                 |
| ------ | ---------------------------------------------------- | ------------------------------------------- |
| GET    | `/api/slots?source=X&limit=N&search=Q`               | List slot summaries (live + persisted)      |
| GET    | `/api/slots/:slot?source=X`                          | Slot detail with time buckets and breakdown |
| GET    | `/api/sources`                                       | List connected probe sources                |
| GET    | `/api/peers?source=X`                                | List peers with connection metadata         |
| GET    | `/api/search?source=X&from_slot=A&to_slot=B&limit=N` | Search persisted slots by range             |

### WebSocket

Connect to `/api/ws?source=X`. The server sends:

- `snapshot` on connect (with `current_slot`)
- `slot_batch` every 100ms with updated slot summaries and current slot

## Persistence

Elapsed slots and source metadata are written to SQLite at `<data-dir>/xray.db`.
Slot summaries and details are stored in SQLite JSONB columns with scalar
`source_id` and `slot` columns for indexed lookup.

On startup, legacy `<data-dir>/<source_id>/source.json` and `slots/*.json` data
is imported into SQLite once. Malformed legacy JSON files are moved under
`<data-dir>/legacy-failed/`, and successfully imported legacy files are removed.
Legacy `index/*.jsonl` files are redundant because summaries are reconstructed
from slot details. Retention pruning runs daily, removing slot rows older than
`--retention-days`.

## Development

```bash
# Build everything
go build ./...

# Run all tests (unit + integration)
go test ./... -timeout 120s

# Dashboard dev server (hot reload)
cd dashboard && bun install && bun run dev

# Dashboard production build
cd dashboard && bun run build

# Regenerate protobuf (requires buf, protoc-gen-go, protoc-gen-connect-go)
buf generate
```

## License

MIT
