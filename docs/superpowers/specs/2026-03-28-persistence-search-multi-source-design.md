# Persistence, search, and multi-source support

This spec covers four interconnected changes to Wiretap: protocol handshake
redesign, peer tracking in the live path, multi-source support with a
dashboard source switcher, and file-based persistence with search.

## Protocol changes

### Handshake

The current protocol is unidirectional: the probe streams events to the
backend with no response. The probe sends a `ServerHello` (misnamed; the
probe is the client) as the first envelope, then a snapshot, then live
events.

The new protocol introduces a bidirectional handshake before the event
stream begins. The probe sends a `ClientHello`, the backend replies with a
`ServerHello` that assigns a stable `source_id`, then the probe continues
with the snapshot and event stream.

```
probe ──── ClientHello ────► backend
probe ◄─── ServerHello ───── backend
probe ──── SnapshotStart ──► backend
probe ──── StringDef* ─────► backend
probe ──── PeerUpsert* ────► backend
probe ──── ConnectionUpsert* ► backend
probe ──── StreamUpsert* ──► backend
probe ──── SnapshotEnd ────► backend
probe ──── events... ──────► backend  (unidirectional from here)
```

Both `ClientHello` and `ServerHello` are sent as varint-delimited protobuf
messages outside the `Envelope` wrapper, since they precede the event
stream. The `Envelope` oneof no longer contains a hello message.

### ClientHello (probe to backend)

```protobuf
message ClientHello {
  uint32 protocol_version = 1;
  bytes peer_id = 2;            // libp2p peer ID of the instrumented node
  string client_name = 3;       // e.g. "prysm/v5.2.0", "lighthouse/v5.3.0"
  bytes boot_id = 4;            // changes each process restart
  int64 started_at_ns = 5;      // probe process start time
}
```

Removed from the old `ServerHello`: `source_id` (now assigned by backend),
`wait_for_attach` (probe-internal, not a protocol concern).

### ServerHello (backend to probe)

```protobuf
message ServerHello {
  uint32 protocol_version = 1;
  string source_id = 2;         // backend-assigned, stable across reconnects
}
```

The backend derives `source_id` deterministically from `peer_id` so the
same probe always receives the same source_id. On reconnection, the
backend recognizes the peer and reassigns the existing source_id, allowing
it to correlate historical data with the reconnected probe.

### Envelope changes

The `Envelope` oneof drops `server_hello`, `snapshot_start`, and
`snapshot_end`. These become standalone messages in the handshake phase.
The envelope carries only runtime events:

```protobuf
message Envelope {
  uint64 seq = 1;
  int64 observed_at_ns = 2;

  oneof payload {
    StringDef string_def = 20;
    PeerUpsert peer_upsert = 21;
    ConnectionUpsert connection_upsert = 22;
    ConnectionClosed connection_closed = 23;
    StreamUpsert stream_upsert = 24;
    StreamClosed stream_closed = 25;
    StreamChunk stream_chunk = 26;
  }
}
```

`SnapshotStart` and `SnapshotEnd` become standalone framing messages sent
between `ServerHello` and the first `Envelope`. They use their own
varint-delimited encoding, same as the hello messages. They can be
distinguished by protobuf tag since `SnapshotStart` and `SnapshotEnd` are
distinct message types with no overlapping semantics.

Actually, a simpler approach: define a `Handshake` wrapper:

```protobuf
message Handshake {
  oneof payload {
    ClientHello client_hello = 1;
    ServerHello server_hello = 2;
    SnapshotStart snapshot_start = 3;
    SnapshotEnd snapshot_end = 4;
  }
}
```

The connection starts in handshake phase (exchanging `Handshake` messages)
then transitions to event phase (exchanging `Envelope` messages). The
probe knows when to switch because it sends `SnapshotEnd` and then begins
sending `Envelope` messages. The backend knows because it receives
`SnapshotEnd` and then expects `Envelope` messages.

During the snapshot phase, `StringDef`, `PeerUpsert`, `ConnectionUpsert`,
and `StreamUpsert` are still sent as `Envelope` messages (they carry seq
numbers and timestamps). So the actual framing is:

```
ClientHello (Handshake)
ServerHello (Handshake)
SnapshotStart (Handshake)
Envelope(StringDef) ...
Envelope(PeerUpsert) ...
Envelope(ConnectionUpsert) ...
Envelope(StreamUpsert) ...
SnapshotEnd (Handshake)
Envelope(StreamChunk) ...   <- live events from here
```

All messages on the wire are varint-delimited protobuf. The reader
determines the phase from context (first two messages are handshake, then
snapshot bracketed by start/end, then envelopes).

## Peer tracking

### Current state

The `Processor` receives `PeerUpsert`, `ConnectionUpsert`, and
`ConnectionClosed` events but ignores them. The dashboard's PEERS tab
shows "Peer data unavailable in live mode."

### Changes

Add a `peers.go` file to `introspector/` that maintains per-source peer
and connection state:

```go
type PeerState struct {
    PeerID      []byte
    Connections map[uint64]*ConnState  // conn_alias -> ConnState
    FirstSeenNs int64
    LastSeenNs  int64
}

type ConnState struct {
    PeerAlias  uint64
    RemoteAddr string
    LocalAddr  string
    Direction  string    // "inbound" or "outbound"
    Transport  string    // resolved from string_def
    Security   string
    Muxer      string
    OpenedAtNs int64
    ClosedAtNs int64     // 0 if still open
}
```

The processor maintains a `peers map[uint64]*PeerState` per source,
keyed by `peer_alias`. On `PeerUpsert`, it creates or updates the entry.
On `ConnectionUpsert`, it adds the connection to the peer's map. On
`ConnectionClosed`, it sets `ClosedAtNs` and removes the connection.

### API

`GET /api/peers?source=<source_id>` returns the live peer list:

```json
{
  "peers": [
    {
      "peer_id": "16Uiu2HAm...",
      "connections": 2,
      "direction": "inbound",
      "remote_addr": "/ip4/1.2.3.4/tcp/9000",
      "first_seen_ns": 1711612800000000000,
      "last_seen_ns": 1711612812000000000
    }
  ]
}
```

The WebSocket `slot_batch` message gains an optional `peer_count` field so
the dashboard can show the peer count without polling.

## Multi-source support

### Source registry

A new `sources.go` manages connected probe sessions:

```go
type SourceInfo struct {
    SourceID    string
    PeerID      []byte
    ClientName  string
    BootID      []byte
    StartedAtNs int64
    ConnectedAt time.Time
    Connected   bool
}
```

`source_id` is derived deterministically from `peer_id`. A simple approach:
base58-encode the peer_id and take a short prefix, or use the full
multibase-encoded peer ID string (which is what libp2p uses as the string
representation). Since peer IDs are already unique, the full string
representation is the cleanest choice.

### Per-source isolation

The `Processor` namespaces all mutable state by source_id:

- `strings map[uint32]string` (string interning table)
- `streams map[uint64]*streamState`
- `peers map[uint64]*PeerState`
- `slots map[uint64]*slotAggregate` (in-memory live slots)

Each source gets its own instance of these maps. Alias values (peer_alias,
conn_alias, stream_alias, string_def ID) are local to a source and can
collide across sources without conflict.

### API changes

All data endpoints gain an optional `source` query parameter:

- `GET /api/slots?source=<id>` (live slot list for this source)
- `GET /api/slots/:slot?source=<id>` (slot detail)
- `GET /api/peers?source=<id>` (peer list)
- `GET /api/search?source=<id>&...` (historical search)

A new endpoint lists available sources:

`GET /api/sources` returns:

```json
{
  "sources": [
    {
      "source_id": "16Uiu2HAm...",
      "client_name": "prysm/v5.2.0",
      "connected": true,
      "connected_at": "2026-03-28T12:00:00Z"
    }
  ]
}
```

This includes both currently connected sources and historically known
sources (from persisted data).

### Dashboard

The header gains a source selector. When only one source is connected,
it shows the client name as static text. When multiple sources exist
(connected or historical), it shows a dropdown. Selecting a source sets
it as the active source; all API calls include `?source=<id>`.

The WebSocket connection also scopes to the active source. On source
switch, the dashboard reconnects the WebSocket with the new source
parameter.

## Persistence

### Storage model

File-per-slot with epoch-grouped summary indices, organized by source:

```
<data_dir>/
  <source_id>/
    slots/
      <slot>.json
    index/
      <epoch>.jsonl
```

Each `<slot>.json` contains the full `SlotDetail` JSON (summary +
breakdowns + bucket time series). This is the same JSON shape that
`/api/slots/:id` returns, so serving a historical slot is a file read
with zero deserialization.

Each `<epoch>.jsonl` is an append-only JSON Lines file containing the
`SlotSummary` for every slot in that epoch (up to 32 lines, ~6.4 KB per
file). This powers the slot list and search for historical data.

Epoch-level grouping provides natural pagination: each epoch is a
page-sized chunk. To list recent slots, read the last few epoch files
in reverse order. For range queries, only read the relevant epoch files.
On Cloudflare, each epoch index maps to one KV entry per source, ideal
for range queries.

On startup, the backend loads recent epoch index files into memory for
fast listing and search. At ~200 bytes per slot summary and 32 slots
per epoch, each epoch file is ~6.4 KB. 30 days of data is ~6,750 epochs
(~43 MB per source). Fits comfortably in memory.

### Write path

When the processor detects that a slot has finalized (current slot has
advanced past it), it serializes the slot data and writes both the slot
file and appends to the index. This happens asynchronously on a
background goroutine to avoid blocking the ingest path.

The in-memory slot map retains the last 256 slots for live dashboard use.
Evicted slots that have been persisted are dropped from memory.

### Read path

- `/api/slots?source=<id>`: reads from in-memory slot map (live data)
- `/api/slots/:slot?source=<id>`: checks in-memory first, falls back to
  reading `<source_id>/slots/<slot>.json`
- `/api/search?source=<id>&...`: queries the in-memory summary index

### Retention

A background goroutine runs daily and removes slot files and epoch index
files older than `--retention-days` (default 30). Since indices are
per-epoch, pruning is deleting entire epoch files (no rewriting).

### Data directory

Default: `~/.wiretap/data/`. Configurable via `--data-dir` flag.

### Cloudflare migration path

The file-per-slot model maps directly to Cloudflare R2 (or any blob
store). The slot file key is `<source_id>/slots/<slot>.json`. The epoch
index key is `<source_id>/index/<epoch>.jsonl`, mapping naturally to one
KV entry per epoch per source. Moving from local filesystem to R2
requires implementing the same read/write interface against the R2 API.
No schema migration, no data transformation.

## Search

### Endpoint

```
GET /api/search?source=<id>&q=<text>&from_slot=N&to_slot=N&limit=N
```

- `source`: required, scopes to one source
- `q`: substring match against slot number and epoch (both derived from
  the slot number)
- `from_slot`, `to_slot`: slot range filter (inclusive)
- `limit`: max results (default 100, max 1000)

Returns `{ "slots": [...] }` where each entry is a `SlotSummary` JSON
object (same shape as the slot list).

### Implementation

The epoch index files are loaded into memory on startup as a sorted slice
of `SlotSummary` values per source. Search is a linear scan with early
termination (the slice is sorted by slot descending). At 216K entries
per source, a full scan takes <10ms. Slot range queries can skip
irrelevant epochs entirely since epoch boundaries are deterministic
(`epoch = slot / 32`).

Richer search criteria (by topic, by bytes threshold, by bleed amount)
can be added later via an async analytics pipeline that reads the slot
files and builds derived indexes. The foundation (raw JSON files) supports
this without migration.

## Code changes

### New files

- `introspector/peers.go`: PeerState, ConnState types and update methods
- `introspector/sources.go`: SourceInfo, source registry, source_id derivation
- `introspector/storage.go`: file-based persistence (write slot, append index, load index, prune)

### Modified files

- `pb/ingest/ingest.proto`: ClientHello, ServerHello, Handshake wrapper,
  Envelope oneof cleanup
- `sink_unix.go`: send ClientHello, receive ServerHello, store assigned
  source_id
- `introspector/ingest.go`: read ClientHello, write ServerHello,
  handshake/snapshot phase handling
- `introspector/processor.go`: per-source state namespacing, peer event
  handling, finalization callback for persistence
- `introspector/server.go`: new endpoints (/api/peers, /api/sources,
  /api/search), source query param on existing endpoints, peer_count in
  WS messages
- `introspector/types.go`: PeerSummary, SourceInfo, updated wsMessage
- `cmd/introspector/main.go`: --data-dir, --retention-days flags, storage
  initialization

### Dashboard changes

- Source selector in header (dropdown when multiple sources exist)
- All API calls include `?source=<active_source_id>`
- PEERS tab populated from `/api/peers` (no longer shows "unavailable")
- Search UI (input field, results displayed in slot list)
- Historical slot detail: shows flow table and chart (full buckets available)

## CLI flags

```
--socket         Unix socket path (default: /tmp/wiretap-introspector.sock)
--listen         HTTP listen address (default: 127.0.0.1:9100)
--genesis-unix   Beacon chain genesis timestamp (default: 1606824023)
--seconds-per-slot  Slot duration (default: 12)
--data-dir       Persistence directory (default: ~/.wiretap/data)
--retention-days Slot retention period (default: 30)
```
