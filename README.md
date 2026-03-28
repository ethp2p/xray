# Ethereum Wiretap

Transparent instrumentation for [go-libp2p](https://github.com/libp2p/go-libp2p) hosts, purpose-built for Ethereum consensus layer analysis. Wraps a `host.Host` to record connection lifecycle, stream activity, and per-protocol bandwidth with zero application code changes.

Includes a real-time web dashboard with per-slot flow visualization, SSZ metadata extraction, and bleed-through detection.

## Quick start

```go
h, _ := libp2p.New()

slotClock := eth.NewSlotClock(time.Unix(1606824023, 0), 12) // mainnet genesis

ih, err := instrument.Wrap(h,
    instrument.WithDashboard(":9100"),
    instrument.WithDecoder(eth.GossipSubDecoder()),
)
// ih implements host.Host; use it everywhere you'd use h
```

Then open the dashboard:

```bash
cd dashboard && bun install && bun run dev
```

Navigate to `http://localhost:5173`. The Vite dev server proxies API requests to the introspector on port 9100.

## Architecture

```
libp2p host
  └─ instrument.Wrap()
       ├─ hot path: byte counting per stream (zero alloc)
       └─ async decode pipeline (off hot path)
            └─ gossipsub RPC parser
                 └─ SSZ metadata extractor (Fulu spec)
                      └─ introspector (per-slot aggregation)
                           ├─ REST API (/api/slots, /api/slots/:id)
                           └─ WebSocket (/api/ws, batched updates)
```

Each stream read/write records byte counts immediately. Protocol-specific decoding happens asynchronously on a separate goroutine per stream, so instrumentation never blocks the application.

## Introspector

Aggregates per-slot bandwidth with 100ms bucket resolution:

- **Breakdown dimensions**: protocol, topic, message kind
- **Message counts**: per breakdown entry (not just bytes)
- **SSZ metadata** (zero-copy offset reads, Fulu consensus-spec):
  - `beacon_block`: proposer index, attestation count, blob KZG commitments, transaction count
  - `blob_sidecar`, `data_column_sidecar`: sidecar index
- **Bleed-through detection**: compares payload slot (from SSZ) against observed slot, bucketed by distance (1, 2, 3, 4+ slots)
- **Decode cache**: FNV-64a content hash avoids repeated Snappy decompression of the same payload from multiple peers
- **Server-side batching**: WebSocket updates collected for 100ms before flushing as a single `slot_batch` message

### API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/slots` | GET | List slot summaries (newest first, up to 256) |
| `/api/slots/:id` | GET | Slot detail with 100ms buckets and breakdown |
| `/api/ws` | WS | Real-time slot updates (`slot_update`, `slot_batch`) |

## Dashboard

Solid.js single-page app. All UI in a single `App.tsx` (~1800 lines).

### Slot list (left panel)

Each slot row is a 2x2 CSS grid:

| Slot number | `in` ████████████ `out` |
|-------------|-------------------------|
| Total KiB | `tx` 142 `blob` 6 `att` 128 `v12345` |

Metadata chips show SSZ-extracted values. The newest slot's total KiB pulses to indicate active traffic. Bandwidth bars use tinted greys (cool for received, warm for sent).

### Diverging area chart (right panel, top)

SVG streamgraph with flows (topics) as layers. Received traffic stacks upward from the zero axis, sent traffic stacks downward. 500ms time grid with second markers.

Flows ordered by slot lifecycle: block, blob, attestation, aggregate, sync committee, exits, slashings. Vertical legend on the right. Hover brings the selected flow to the top z-order.

Topic grouping collapses indexed subnets: `beacon_attestation_0..63` into `attestation`, `blob_sidecar_0..5` into `blob_sidecar`, `data_column_sidecar_0..127` into `data_column_sidecar`.

**Bleed-through overlay**: Hatched diagonal pattern on the outer edge of each flow's area, indicating traffic from previous slots. When hovered, tints from pink (1 slot) to deep red (4+ slots).

**Tooltips**: Follow cursor, show total (slot aggregate) and spot (time point) traffic. Bleed sections include distance histograms with colored proportion bars.

### Breakdown tables (right panel, bottom)

**Flow table**: Per-flow rows with columns:
- NAME (with expand chevron and color dot)
- DOMAIN (GSUB or RPC chip)
- DATA (received/sent KiB)
- CONTROL (IHAVE, IWANT, GRAFT, PRUNE, IDONTWANT; dynamic columns)
- BLEED (received/sent KiB, red-tinted column)

Expandable: grouped flows (e.g., `attestation`) expand to show individual subnet topics.

**Protocol overhead table**: Gossipsub framing bytes and topic-less control traffic (IWANT, IDONTWANT). Subtle grey background to distinguish from the flow table.

### Interaction

- **Bidirectional hover**: chart layers highlight table rows and vice versa
- **SIM/LIVE toggle** (`m`): mock data for development, live WebSocket for production
- **Keyboard**: `j`/`k` slot navigation, `m` mode, `t` theme, `Cmd+K` command palette

### Stack

Solid.js, TypeScript (strict), Iosevka mono, SVG, Vite

## Running tests

```bash
go test ./...
```

## License

MIT
