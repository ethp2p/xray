# Wiretap

Transparent libp2p network instrumentation with real-time analysis dashboard for Ethereum consensus layer research.

Wiretap wraps any `go-libp2p` host to capture stream-level traffic without modifying application code. A separate backend process decodes gossipsub messages, extracts SSZ slot numbers, and aggregates per-slot bandwidth breakdowns. A Solid.js dashboard renders the data in real time.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Ethereum CL client                       │
│                  (Prysm, Lighthouse, etc.)                    │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                    Probe (probe/)                        │ │
│  │  Wraps go-libp2p Host, intercepts streams/connections    │ │
│  │  Forwards raw bytes via ingest protocol                  │ │
│  └──────────────────────┬──────────────────────────────────┘ │
└─────────────────────────┼───────────────────────────────────┘
                          │ Unix socket / TCP
                          │ (ClientHello -> ServerHello -> Envelopes)
                          v
┌─────────────────────────────────────────────────────────────┐
│                   Backend (cmd/wiretap)                       │
│                                                               │
│  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌────────────┐ │
│  │ Ingest   │->│ Processor │->│ Storage  │  │ HTTP/WS    │ │
│  │ listener │  │ (per-src) │  │ (files)  │  │ server     │ │
│  └──────────┘  └───────────┘  └──────────┘  └─────┬──────┘ │
│                                                     │        │
│  gossipsub/ --- RPC parser                          │        │
│  eth/ ────────- SSZ decoder, slot clock             │        │
└─────────────────────────────────────────────────────┼───────┘
                                                      │
                                              ┌───────v───────┐
                                              │   Dashboard    │
                                              │  (Solid.js)    │
                                              │  localhost:5173 │
                                              └───────────────┘
```

The **probe** is a library that clients embed. It wraps the libp2p `Host`, intercepts every `Read`/`Write` on every stream, and forwards raw byte chunks over a lightweight ingest protocol to the backend. The probe has no Ethereum-specific logic; it sends opaque bytes.

The **backend** is a standalone binary (`cmd/wiretap`). It accepts probe connections, reassembles gossipsub RPC frames, decodes SSZ payloads to extract slot numbers and block metadata, then aggregates traffic into 100ms time buckets per slot. It serves a REST + WebSocket API for the dashboard and persists finalized slots to disk.

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
go build -o wiretap ./cmd/wiretap
./wiretap --ingest=/tmp/wiretap.sock --listen=127.0.0.1:9100
```

Start the dashboard dev server:

```bash
cd dashboard && bun install && bun run dev
```

Open `http://localhost:5173`. Vite proxies API requests to the backend on `:9100`.

### Integrate with Prysm

```go
import "github.com/ethp2p/wiretap/probe"

ih, err := probe.Wrap(h,
    probe.WithIngestAddr("/tmp/wiretap.sock"),
    probe.WithClientName("prysm"),
    probe.WithWaitForAttach(),
)
```

Prysm's fork supports this via `--instrument-socket` and `--instrument-file` flags.

## Project structure

```
probe/                  Library clients import (host wrapper, sinks, emitter)
eth/                    Ethereum: slot clock, SSZ extraction, gossipsub decoder
gossipsub/              Gossipsub RPC parser (varint framing, action atomization)
backend/                Per-slot aggregation, REST/WS API, processor, storage
wire/                   Ingest protocol codec (typed length-delimited framing)
proto/                  Protobuf definitions and generated code
  ingest/               Ingest protocol messages (Envelope, ClientHello, etc.)
cmd/wiretap/            Backend binary entrypoint
itest/                  Integration tests (gossipsub decoding, introspector E2E)
dashboard/              Solid.js web dashboard
docs/                   Specs and plans
```

## Probe integration

The probe wraps a `go-libp2p` host transparently:

```go
import "github.com/ethp2p/wiretap/probe"

host, _ := libp2p.New(...)
ih, err := probe.Wrap(host,
    probe.WithIngestAddr("/tmp/wiretap.sock"),
    probe.WithClientName("my-client/v1.0"),
    probe.WithWaitForAttach(),                    // block until backend connects
    probe.WithSinkFile("/var/log/wiretap.trace"), // optional local trace file
    probe.WithDecoder(gossipsub.Decoder{}.Match, gossipsub.Decoder{}.New),
    probe.WithOnMessage(func(streamID uint32, protocol string) probe.OnMessage {
        return func(msg probe.DecodedMessage) {
            // handle decoded messages off the hot path
        }
    }),
)
defer ih.Close()
```

`probe.Wrap` returns a `*probe.Host` that satisfies `host.Host`. Existing code works unchanged; all stream reads/writes are intercepted and forwarded.

## Configuration

Backend CLI flags (`cmd/wiretap`):

| Flag | Default | Description |
|------|---------|-------------|
| `--ingest` | `/tmp/wiretap.sock` | Ingest listener address (Unix path or host:port) |
| `--listen` | `127.0.0.1:9100` | HTTP listen address for REST/WS API |
| `--data-dir` | `~/.wiretap/data` | Persistence directory for slot data |
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

Finalized slots are written to disk under `<data-dir>/<source_id>/`:

```
<source_id>/
  source.json                 Source metadata (peer ID, client name)
  slots/<slot>.json           Full slot detail (summary + buckets + breakdown)
  index/<epoch>.jsonl         One SlotSummary JSON per line, append-only
```

Retention pruning runs daily, removing slot files and epoch indices older than `--retention-days`.

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
