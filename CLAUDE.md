# Wiretap

Go instrumentation library for libp2p with a Solid.js real-time dashboard for Ethereum consensus layer analysis.

## Project structure

```
├── *.go                    # Core instrument package (host wrapper, sinks, emitter)
├── eth/                    # Ethereum-specific: gossipsub decoder, SSZ extraction, slot clock
├── introspector/           # Per-slot aggregation, REST/WS API, processor
├── libp2p/gossipsub/       # Gossipsub RPC parser (varint framing, action atomization)
├── serve/                  # WebSocket and gRPC endpoints
├── pb/                     # Protobuf definitions and generated code
├── cmd/introspector/       # Introspector binary entrypoint
├── itest/                  # Integration tests
└── dashboard/              # Solid.js web dashboard
    ├── src/App.tsx          # Entire UI (~1800 lines, single file)
    ├── src/index.css         # CSS reset, type scale, animations
    ├── src/main.tsx          # Solid.js render entrypoint
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

Vite proxies `/api/*` and `/ws` to `localhost:9100` (the introspector).

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

All text uses Iosevka monospace (`var(--m)`). Font loaded from jsDelivr CDN.

### Theme system

Two themes (dark/light) defined as `THEME_VARS` in App.tsx. CSS custom properties applied to `document.documentElement` via a `createEffect`. Key tokens:

- `--0` through `--3`: background shades (darkest to lightest)
- `--fg`, `--hi`: foreground and highlight text
- `--sel-bg`, `--sel-border`, `--hover`: selection and interaction states
- `--proto-gsub`, `--proto-reqr`, `--proto-dv5`, `--proto-eth`: protocol colors

Flow colors use `topicColor()` which assigns stable HSL hues via string hashing. Known topics get fixed hues from `TOPIC_HUES`; unknown topics get a hash-based fallback.

### Data model

**Flows**: A flow is a unique topic (e.g., `attestation`, `beacon_block`). Topics are normalized from raw API paths (`/eth2/{hash}/beacon_attestation_15/ssz_snappy` becomes `beacon_attestation_15`) then grouped by `TOPIC_GROUP_RULES` (attestation subnets collapse to `attestation`, blob sidecars to `blob_sidecar`, etc.).

**FlowTimePoint**: Per-bucket data for the chart. Each flow has `{ in, out, bleedIn, bleedOut, bleedByDist }`.

**FlowRow**: Per-slot aggregate for the table. Includes `dataIn/Out`, `bleedIn/Out`, `control` (map of message_kind to in/out), `bleedByDist`, and `children` (individual topics within a group).

### SIM vs LIVE mode

- **SIM** (`dataMode === "sim"`): Mock data from seeded RNG. No backend needed. Auto-ticks every 5s.
- **LIVE** (`dataMode === "live"`): WebSocket to `/api/ws` with 120ms client-side batching. REST fetches for slot list and detail. Slot selection triggers `/api/slots/:id` fetch.

Toggle with `m` key or command palette.

### Key patterns

- `<For>` with `on:click` for slot rows (not `.map()` with `onClick`): prevents click loss during WS-triggered re-renders
- Tooltip follows cursor via `mousePos` signal, clamped to chart bounds
- Bleed overlay paths have their own mouse events for hover detection
- Highlighted flow's SVG paths sorted to render last (SVG z-order = document order)
- `computeDiverging` is generic over data type via accessor functions
- Protocol overhead separated from flows by checking `isGossipProtocol()`

## Go backend

### Introspector

`introspector/processor.go` aggregates traffic by slot with 100ms buckets. Each `BucketBreakdown` has `protocol`, `topic`, `message_kind`, `bytes_in`, `bytes_out`, `msg_count`, `bleed_bytes_in`, `bleed_bytes_out`, `bleed_by_distance`.

Bleed detection: compares `eth.payload.slot` (from SSZ) against the observed slot. Distance bucketed as "1", "2", "3", "4+".

### SSZ extraction (`eth/ssz_meta.go`, `eth/ssz_slot.go`)

Zero-copy offset reads from Snappy-decompressed gossipsub payloads. Offsets verified against Fulu consensus-spec:

- `beacon_block`: slot at +100, proposer_index at +108, then follows body_offset to read attestation/blob_kzg/tx list lengths
- `blob_sidecar`: index at +0, slot at +131176
- `data_column_sidecar`: index at +0, slot at +20

Content-addressed decode cache (FNV-64a hash, 256 entries) avoids re-decompressing the same payload from multiple peers.

### WebSocket batching (`introspector/server.go`)

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
