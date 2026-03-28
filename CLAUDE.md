# Wiretap

Go instrumentation library for libp2p with a Solid.js real-time dashboard ("Ethereum Xray") for Ethereum consensus layer analysis. Live at [xray.ethp2p.dev](https://xray.ethp2p.dev).

## Project structure

```
├── probe/                  # Library clients import (host wrapper, sinks, emitter)
├── eth/                    # Ethereum-specific: gossipsub decoder, SSZ extraction, slot clock
├── gossipsub/              # Gossipsub RPC parser (varint framing, action atomization)
├── backend/                # Per-slot aggregation, REST/WS API, processor
├── wire/                   # Shared protocol codec (ingest framing)
├── proto/                  # Protobuf definitions and generated code
│   └── ingest/
├── cmd/wiretap/            # Backend binary entrypoint
├── itest/                  # Integration tests
└── dashboard/              # Solid.js web dashboard ("Ethereum Xray")
    ├── src/App.tsx          # Entire UI (~2100 lines, single file)
    ├── src/index.css         # CSS reset, type scale, animations
    ├── src/main.tsx          # Solid.js render entrypoint
    ├── public/              # Static assets (favicon, OG image, Iosevka woff2)
    └── vite.config.ts        # Vite dev server, proxies /api to :9100
```

## Build and run

```bash
# Go
go build ./...
go test ./...

# Dashboard
cd dashboard && bun install && bun run dev   # dev server on :5173
cd dashboard && bun run build                # production build
```

Vite proxies `/api/*` and `/ws` to `localhost:9100` (the backend).

## Dashboard architecture

**Everything is in `App.tsx`.** There are no extracted components, no state management library, no router. The file contains:

- Data types and mock data generators (sim mode)
- API types matching the Go backend JSON
- Flow derivation (groupTopic, deriveFlows, deriveFlowTimeSeries)
- SVG diverging chart (computeDiverging, StreamGraph component)
- Slot list with 2x2 CSS grid rows
- Flow breakdown table with expandable children
- Protocol overhead table
- Bleed-through visualization (hatched SVG pattern, distance histograms)
- WebSocket connection with rAF batching
- Command palette, keyboard shortcuts

### Framework: Solid.js (not React)

- `createSignal`, `createMemo`, `createEffect` (not useState/useMemo/useEffect)
- `<Show>`, `<For>` (not ternaries and .map())
- `on:click` for native events (not onClick which is delegated)
- Props accessed via `props.x` (never destructured)
- Component bodies run once; reactivity is through signal accessors

**Critical**: use `on:click` (native) not `onClick` (delegated) for elements that may be recreated during WebSocket updates. Delegated events lose their target when DOM nodes are replaced between mousedown and mouseup.

### Type scale (CSS custom properties in index.css)

| Token | Size | Usage |
|-------|------|-------|
| `--t-xs` | 10px | Axis ticks, tertiary labels |
| `--t-sm` | 12px | Attributes, controls, kbd hints |
| `--t-md` | 13px | Primary data, slot numbers, totals |
| `--t-lg` | 16px | Section headers, title |

All text uses Iosevka monospace (`var(--m)`). Font self-hosted as woff2 in `public/iosevka-400.woff2`. No `local()` source; browsers must download the file to avoid phantom font matching.

### Theme system

Two themes (dark/light) defined as `THEME_VARS` in App.tsx. CSS custom properties applied to `document.documentElement` via a `createEffect`. Key tokens:

- `--0` through `--3`: background shades (darkest to lightest)
- `--fg`, `--hi`: foreground and highlight text
- `--sel-bg`, `--sel-border`, `--hover`: selection and interaction states
- `--proto-gsub`, `--proto-reqr`, `--proto-dv5`, `--proto-eth`: protocol colors

Flow colors use `topicColor()` which assigns stable HSL hues via `TOPIC_HUES`. Key assignments: beacon_block=orange(30), attestation=cyan(195), beacon_aggregate_and_proof=purple(275), blob_sidecar=teal(140), data_column_sidecar=green(100), sync_committee_contribution_and_proof=yellow(55). Unknown topics get a hash-based fallback hue.

### Data model

**Flows**: A flow is a unique topic (e.g., `attestation`, `beacon_block`). Topics are normalized from raw API paths (`/eth2/{hash}/beacon_attestation_15/ssz_snappy` becomes `beacon_attestation_15`) then grouped by `TOPIC_GROUP_RULES` (attestation subnets collapse to `attestation`, blob sidecars to `blob_sidecar`, etc.).

**FlowTimePoint**: Per-bucket data for the chart. Each flow has `{ in, out, bleedIn, bleedOut, bleedByDist }`.

**FlowRow**: Per-slot aggregate for the table. Includes `dataIn/Out`, `bleedIn/Out`, `control` (map of message_kind to in/out), `bleedByDist`, and `children` (individual topics within a group).

### SIM vs LIVE mode

- **LIVE**: WebSocket to `/api/ws` with 120ms client-side batching. REST fetches for slot list and detail. Slot selection triggers `/api/slots/:id` fetch.

### Key patterns

- `<For>` with `on:click` for slot rows (not `.map()` with `onClick`): prevents click loss during WS-triggered re-renders
- Tooltip follows cursor via `mousePos` signal, clamped to chart bounds
- Bleed overlay paths have their own mouse events for hover detection
- Highlighted flow's SVG paths sorted to render last (SVG z-order = document order)
- `computeDiverging` is generic over data type via accessor functions; returns `ceil` and `scale` for reuse by hit testing and y-axis ticks
- Protocol overhead separated from flows by checking `isGossipProtocol()`
- Chart legend is fixed (not data-driven), split into four sections: Broadcast, RPC, Overhead, Markers
- Y-axis ceilings and ticks are always powers of two (8 KiB through 64 MiB)
- Direction labels (RCVD/SENT) rendered on both left and right edges at z-index 5

## Go backend

### Processor

`backend/processor.go` aggregates traffic by slot with 100ms buckets. Each `BucketBreakdown` has `protocol`, `topic`, `message_kind`, `bytes_in`, `bytes_out`, `msg_count`, `bleed_bytes_in`, `bleed_bytes_out`, `bleed_by_distance`.

Bleed detection: compares `eth.payload.slot` (from SSZ) against the observed slot. Distance bucketed as "1", "2", "3", "4+".

### SSZ extraction (`eth/ssz_meta.go`, `eth/ssz_slot.go`)

Zero-copy offset reads from Snappy-decompressed gossipsub payloads. Offsets verified against Fulu consensus-spec:

- `beacon_block`: slot at +100, proposer_index at +108, then follows body_offset to read attestation/blob_kzg/tx list lengths
- `blob_sidecar`: index at +0, slot at +131176
- `data_column_sidecar`: index at +0, slot at +20

Content-addressed decode cache (FNV-64a hash, 256 entries) avoids re-decompressing the same payload from multiple peers.

### WebSocket batching (`backend/server.go`)

Server collects slot updates for 100ms via `time.AfterFunc`, then sends one `slot_batch` message. Snapshot written before client registration to prevent concurrent WebSocket writes (gorilla/websocket requires single-writer).

## Design context

### Users

Ethereum protocol researchers and P2P network engineers. They think in slots and epochs, read hex dumps, and use terminal tools daily. Speed of comprehension over onboarding.

### Design principles

1. **Data over chrome.** Every pixel is data, axis, label, or interaction affordance. No decoration.
2. **Structure through type.** Font weight, size, case, and spacing create hierarchy. Minimize borders and fills.
3. **Color means something.** Every color maps to a data concept. Never decorative.
4. **Keyboard-first.** Single-key shortcuts. Mouse supported but not required.

### Anti-references

No SaaS dashboards, no Material UI, no glassmorphism, no gradient text, no hero metrics, no card grids. If it looks AI-generated, it's wrong.
