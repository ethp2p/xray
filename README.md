# Xray

Real-time analysis of network utilization of Ethereum Consensus Layer nodes.

**Live dashboard**: [xray.ethp2p.dev](https://xray.ethp2p.dev)

## How it works

Xray wraps the libp2p Host to capture stream-level traffic without modifying application code.

A separate backend process decodes gossipsub messages, extracts SSZ slot numbers, and aggregates per-slot bandwidth breakdowns.

A Solid.js dashboard ("Ethereum Xray") renders the data in real time.

## Architecture

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
│  gossipsub/ --- RPC parser                        │         │
│  eth/ ────────- SSZ decoder, slot clock           │         │
└───────────────────────────────────────────────────┼─────────┘
                                                    │
                                            ┌───────v─────────┐
                                            │  Xray Dashboard │
                                            │  (Solid.js)     │
                                            └─────────────────┘
```

The **producer library** is what clients embed. It wraps the libp2p `Host`, intercepts every `Read`/`Write` on every stream, and forwards raw byte chunks over a lightweight ingest protocol to the backend. The probe has no Ethereum-specific logic; it sends opaque bytes.

The **backend** is a standalone binary (`cmd/xray`). It accepts probe connections, reassembles gossipsub RPC frames, decodes SSZ payloads to extract slot numbers and block metadata, then aggregates traffic into 100ms time buckets per slot. It serves a REST + WebSocket API for the dashboard and persists finalized slots and source metadata to SQLite.

The **dashboard** is a Solid.js single-page app that connects to the backend over WebSocket for live slot updates and REST for historical data.

## Installation

### Docker Compose

Docker Compose is the quickest way to install the Xray backend and dashboard.
You need Git and a current Docker installation with Compose.

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

Open `http://127.0.0.1:9100`. Compose publishes the dashboard only on
loopback. Put a local reverse proxy or tunnel in front of it if remote users
need access.

The backend creates its ingest socket at `$XRAY_SOCK_DIR/xray.sock` on the
host. Configure the instrumented client to use that path. Stop Xray with
`docker compose down`. Its SQLite data remains in `$XRAY_DATA_DIR`.

### Build from source

Source builds need Go 1.25, CGO, a C compiler, and the SQLite development
headers. Building the dashboard also needs Bun.

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

### Install the production stack

The `infra/` directory defines the production stack as Podman Quadlets
managed by systemd:

- Nethermind execution client
- the instrumented Prysm fork
- Xray backend and dashboard
- the shared runtime socket directory

See [`infra/README.md`](infra/README.md) for pinned versions, image builds,
installation, rollout, verification, upgrades, and rollback. The checked-in
deployment targets Podman 4.9 on Ubuntu and keeps the JSON-RPC, Engine,
Prysm API, and Xray dashboard ports on loopback.

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
