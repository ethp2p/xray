# Wiretap

Transparent libp2p network instrumentation with real-time analysis dashboard for Ethereum consensus layer research.

**Live dashboard**: [xray.ethp2p.dev](https://xray.ethp2p.dev)

Wiretap wraps any `go-libp2p` host to capture stream-level traffic without modifying application code. A separate backend process decodes gossipsub messages, extracts SSZ slot numbers, and aggregates per-slot bandwidth breakdowns. A Solid.js dashboard ("Ethereum Xray") renders the data in real time.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Ethereum CL client                       │
│                  (Prysm, Lighthouse, etc.)                    │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                    Wiretap (xray module root)            │ │
│  │  Wraps go-libp2p Host, intercepts streams/connections    │ │
│  │  Forwards raw bytes via ingest protocol                  │ │
│  └──────────────────────┬──────────────────────────────────┘ │
└─────────────────────────┼───────────────────────────────────┘
                          │ Unix socket / TCP
                          │ (ClientHello -> ServerHello -> Envelopes)
                          v
┌─────────────────────────────────────────────────────────────┐
│                   Backend (cmd/xray)                          │
│                                                               │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌────────────┐  │
│  │ Ingest   │->│ Processor │->│ Storage  │  │ HTTP/WS    │  │
│  │ listener │  │ (per-src) │  │ (SQLite) │  │ server     │  │
│  └──────────┘  └───────────┘  └──────────┘  └─────┬──────┘  │
│                                                     │        │
│  gossipsub/ --- RPC parser                          │        │
│  eth/ ────────- SSZ decoder, slot clock             │        │
└─────────────────────────────────────────────────────┼───────┘
                                                      │
                                              ┌───────v───────┐
                                              │  Xray Dashboard│
                                              │  (Solid.js)    │
                                              │  localhost:5173 │
                                              └───────────────┘
```

The **probe** is a library that clients embed. It wraps the libp2p `Host`, intercepts every `Read`/`Write` on every stream, and forwards raw byte chunks over a lightweight ingest protocol to the backend. The probe has no Ethereum-specific logic; it sends opaque bytes.

The **backend** is a standalone binary (`cmd/xray`). It accepts probe connections, reassembles gossipsub RPC frames, decodes SSZ payloads to extract slot numbers and block metadata, then aggregates traffic into 100ms time buckets per slot. It serves a REST + WebSocket API for the dashboard and persists finalized slots and source metadata to SQLite.

The **dashboard** is a Solid.js single-page app that connects to the backend over WebSocket for live slot updates and REST for historical data.

## Quick start

### Run with Docker Compose

```bash
docker compose up --build
```

The backend listens on port 9100. Mount the probe's Unix socket directory as a volume (see `compose.yaml`).

### Run locally

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

```go
import "github.com/ethp2p/xray"

ih, err := xray.Wiretap(h,
    xray.WithIngestAddr("/tmp/xray.sock"),
    xray.WithClientName("prysm"),
    xray.WithWaitForAttach(),
)
```

Prysm's fork supports this via `--instrument-socket` and `--instrument-file` flags.

## Project structure

```
*.go                    Wiretap producer SDK (host wrapper, sinks, emitter) at module root
api/                    Shared JSON DTOs for REST/WebSocket responses
cmd/xray/               Backend binary entrypoint
internal/eth/           Ethereum slot clock and SSZ extraction
internal/gossipsub/     Gossipsub RPC parser
internal/ingest/        Probe connection listener and ingest sessions
internal/processor/     Per-source aggregation and finalized slot production
internal/server/        REST/WebSocket API server
internal/sources/       Probe source registry
internal/storage/       SQLite persistence for sources and finalized slots
proto/wiretap/          Protobuf definitions and generated ingest messages
proto/wiretap/wire/     Typed length-delimited ingest protocol codec
itest/                  Integration tests (gossipsub decoding, introspector E2E)
dashboard/              Solid.js web dashboard ("Ethereum Xray")
clients/                Example or integration client code
gen/                    Generated support code
```

## Probe integration

The probe wraps a `go-libp2p` host transparently:

```go
import "github.com/ethp2p/xray"

host, _ := libp2p.New(...)
ih, err := xray.Wiretap(host,
    xray.WithIngestAddr("/tmp/xray.sock"),
    xray.WithClientName("my-client/v1.0"),
    xray.WithWaitForAttach(),                    // block until backend connects
    xray.WithSinkFile("/var/log/xray.trace"),    // optional local trace file
    xray.WithDecoder(gossipsub.Decoder{}.Match, gossipsub.Decoder{}.New),
    xray.WithOnMessage(func(streamID uint32, protocol string) xray.OnMessage {
        return func(msg xray.DecodedMessage) {
            // handle decoded messages off the hot path
        }
    }),
)
defer ih.Close()
```

`xray.Wiretap` returns a `*xray.Host` that satisfies `host.Host`. Existing code works unchanged; all stream reads/writes are intercepted and forwarded.

## Configuration

Backend CLI flags (`cmd/xray`):

| Flag | Default | Description |
|------|---------|-------------|
| `--ingest` | `/tmp/xray.sock` | Ingest listener address (Unix path or host:port) |
| `--listen` | `127.0.0.1:9100` | HTTP listen address for REST/WS API |
| `--data-dir` | `~/.xray/data` | Persistence directory for slot data |
| `--retention-days` | `30` | Slot retention period in days |
| `--genesis-unix` | `1606824023` | Beacon chain genesis Unix timestamp |
| `--seconds-per-slot` | `12` | Beacon chain seconds per slot |
| `--static-dir` | (none) | Serve dashboard static files from this directory |

## API

### REST endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/slots?source=X&limit=N&search=Q` | List slot summaries (live + persisted) |
| GET | `/api/slots/:slot?source=X` | Slot detail with time buckets and breakdown |
| GET | `/api/sources` | List connected probe sources |
| GET | `/api/peers?source=X` | List peers with connection metadata |
| GET | `/api/search?source=X&from_slot=A&to_slot=B&limit=N` | Search persisted slots by range |

### WebSocket

Connect to `/api/ws?source=X`. The server sends:

- `snapshot` on connect (with `current_slot`)
- `slot_batch` every 100ms with updated slot summaries and current slot

## Persistence

Finalized slots and source metadata are written to SQLite at `<data-dir>/xray.db`.
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
