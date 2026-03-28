# Persistence, search, and multi-source support

This spec covers four interconnected changes to Wiretap: protocol handshake
redesign, peer tracking in the live path, multi-source support with a
dashboard source switcher, and file-based persistence with search.

Both sides of the protocol (probe and backend) are deployed together from
the same repo. There is no cross-version compatibility requirement. Protocol
changes are breaking and both sides update in lockstep.

## Protocol changes

### Connection topology

The current architecture has the probe listening on a Unix socket and the
backend dialing in. This inverts for multi-source: the backend listens on
a socket (Unix or TCP), and probes dial in. This allows multiple probes to
connect to a single backend simultaneously.

The `--socket` flag on the backend becomes the listen address. The probe's
`SinkUnix` changes from listener to dialer: it connects to the backend's
socket and sends `ClientHello` as the first message.

### Handshake

The probe sends a `ClientHello`, the backend replies with a `ServerHello`
that assigns a stable `source_id`, then the probe sends a snapshot and
transitions to streaming events.

```
probe ──── ClientHello ────► backend
probe ◄─── ServerHello ───── backend
probe ──── Envelope(SnapshotStart) ──► backend
probe ──── Envelope(StringDef) ... ──► backend
probe ──── Envelope(PeerUpsert) ... ──► backend
probe ──── Envelope(ConnectionUpsert) ... ──► backend
probe ──── Envelope(StreamUpsert) ... ──► backend
probe ──── Envelope(SnapshotEnd) ──► backend
probe ──── Envelope(StreamChunk) ... ──► backend
```

### Wire framing

All messages use varint-delimited protobuf. To avoid ambiguity between
message types, the wire uses a single-byte type discriminator before each
varint-delimited message:

- `0x01`: `ClientHello`
- `0x02`: `ServerHello`
- `0x03`: `Envelope`

The reader checks the discriminator byte, then reads the varint length
and deserializes into the corresponding type. The handshake phase reads
`0x01` (ClientHello) then writes `0x02` (ServerHello). All subsequent
messages from the probe are `0x03` (Envelope).

This eliminates the need for a `Handshake` wrapper message. Each type is
a standalone protobuf message.

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

### ServerHello (backend to probe)

```protobuf
message ServerHello {
  uint32 protocol_version = 1;
  string source_id = 2;         // backend-assigned, stable across reconnects
}
```

If the backend does not support the probe's protocol version, it closes
the connection immediately after reading `ClientHello` (no `ServerHello`
sent). The probe treats a connection close before receiving `ServerHello`
as a version mismatch and logs the error.

The backend derives `source_id` deterministically from `peer_id`: the
full multibase-encoded peer ID string (the standard libp2p string
representation). Same peer always gets the same source_id.

### Envelope changes

`SnapshotStart` and `SnapshotEnd` remain in the `Envelope` oneof. They
carry seq numbers and timestamps like all other events. The `Envelope`
oneof drops only `server_hello` (field 10):

```protobuf
message Envelope {
  uint64 seq = 1;
  int64 observed_at_ns = 2;

  oneof payload {
    SnapshotStart snapshot_start = 11;
    SnapshotEnd snapshot_end = 12;

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

The backend reads `Envelope` messages in a loop after the handshake.
It tracks the session phase internally: on `SnapshotStart` it enters
snapshot mode, on `SnapshotEnd` it transitions to live mode. During
snapshot mode it processes `StringDef`, `PeerUpsert`, `ConnectionUpsert`,
and `StreamUpsert` to rebuild state. `StreamChunk` events during snapshot
mode are ignored (the probe should not send them, but the backend is
tolerant).

### Reconnection semantics

When a probe reconnects (same `peer_id`, new TCP connection), the backend:

1. Assigns the same `source_id` (deterministic from `peer_id`)
2. Compares `boot_id` to the previous session
3. If `boot_id` changed (probe restarted): clears `strings`, `streams`,
   and `peers` maps for this source. The new snapshot will rebuild them.
   In-memory `slots` are kept (they are independent of aliases).
4. If `boot_id` is the same (network interruption): same behavior (clear
   and rebuild from snapshot), since alias numbering may have diverged.

The fresh snapshot after reconnection is the authoritative state.

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
}
```

The processor maintains a `peers map[uint64]*PeerState` per source,
keyed by `peer_alias`. On `PeerUpsert`, it creates or updates the entry.
On `ConnectionUpsert`, it adds the connection to the peer's map. On
`ConnectionClosed`, it removes the connection from the peer's map. If a
peer has zero remaining connections, the peer entry is removed.

### API

`GET /api/peers?source=<source_id>` returns the live peer list:

```json
{
  "peers": [
    {
      "peer_id": "16Uiu2HAm...",
      "connections": [
        {
          "remote_addr": "/ip4/1.2.3.4/tcp/9000",
          "direction": "inbound",
          "transport": "tcp",
          "opened_at_ns": 1711612800000000000
        }
      ],
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

`source_id` is the full multibase-encoded peer ID string (the standard
libp2p string representation). Since peer IDs are already unique, no
additional hashing or prefix extraction is needed.

### Per-source isolation

The `Processor` holds a `sourceState` struct per source_id:

```go
type sourceState struct {
    info    SourceInfo
    strings map[uint32]string
    streams map[uint64]*streamState
    peers   map[uint64]*PeerState
    slots   map[uint64]*slotAggregate
}
```

Each source gets its own instance. Alias values (peer_alias, conn_alias,
stream_alias, string_def ID) are local to a source and can collide across
sources without conflict.

### Connection handling

The backend listens on the configured socket. Each incoming connection
gets its own goroutine that handles the handshake and event loop. The
`Processor` is thread-safe (already uses `sync.RWMutex`). Multiple probes
can connect simultaneously; each is isolated by source_id.

### API changes

All data endpoints require a `source` query parameter:

- `GET /api/slots?source=<id>` (live slot list for this source)
- `GET /api/slots/:slot?source=<id>` (slot detail)
- `GET /api/peers?source=<id>` (peer list)
- `GET /api/search?source=<id>&...` (historical search)

When `source` is omitted: if exactly one source exists (connected or
historical), it is used as the default. Otherwise, the endpoint returns
400 with an error listing available sources.

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
sources (discovered from persisted data directories on startup).

### WebSocket source scoping

The WebSocket endpoint accepts a source parameter: `/api/ws?source=<id>`.
The backend only sends updates for the specified source on that connection.
On source switch, the dashboard closes the current WebSocket and opens a
new one with the new source parameter.

### Dashboard

The header gains a source selector. When only one source exists, it shows
the client name as static text. When multiple sources exist (connected or
historical), it shows a dropdown. Selecting a source sets it as the active
source; all API calls include `?source=<id>`.

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
file and appends to the epoch index. This happens asynchronously on a
background goroutine to avoid blocking the ingest path.

Crash safety: slot files are written to a temporary file in the same
directory then renamed into place (atomic on POSIX). The epoch index
append is not atomic, but a truncated last line is detected and discarded
on load (each line is independently valid JSON).

The in-memory slot map retains the last 256 slots for live dashboard use.
Evicted slots that have been persisted are dropped from memory.

### Read path

- `/api/slots?source=<id>`: reads from in-memory slot map (live data)
- `/api/slots/:slot?source=<id>`: checks in-memory first, falls back to
  reading `<source_id>/slots/<slot>.json`
- `/api/search?source=<id>&...`: queries the in-memory summary index

### Retention

A background goroutine runs daily and removes slot files and epoch index
files older than `--retention-days` (default 30). An epoch file is deleted
only when all its slots fall outside the retention window. Since epochs
are ~6.4 minutes, this is at most one epoch of extra retention at the
boundary.

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
GET /api/search?source=<id>&from_slot=N&to_slot=N&limit=N
```

- `source`: required, scopes to one source
- `from_slot`, `to_slot`: slot range filter (inclusive)
- `limit`: max results (default 100, max 1000)

Returns `{ "slots": [...] }` where each entry is a `SlotSummary` JSON
object (same shape as the slot list).

### Implementation

The epoch index files are loaded into memory on startup as a sorted slice
of `SlotSummary` values per source. Search is a binary search on the
sorted slice (by slot number) followed by linear scan within the range.
Slot range queries skip irrelevant epochs since epoch boundaries are
deterministic (`epoch = slot / 32`).

Richer search criteria (by topic, by bytes threshold, by bleed amount)
can be added later via an async analytics pipeline that reads the slot
files and builds derived indexes. The foundation (raw JSON files) supports
this without migration.

## Code changes

### New files

- `introspector/peers.go`: PeerState, ConnState types and update methods
- `introspector/sources.go`: SourceInfo, source registry, source_id derivation
- `introspector/storage.go`: file-based persistence (write slot, append
  index, load index, prune, write-to-temp-then-rename)

### Modified files

- `pb/ingest/ingest.proto`: ClientHello, ServerHello (new messages),
  Envelope oneof drops server_hello field, type discriminator constants
- `sink_unix.go`: changes from listener to dialer, sends ClientHello,
  receives ServerHello, stores assigned source_id
- `introspector/ingest.go`: changes from dialer to listener, accepts
  multiple connections, reads ClientHello, writes ServerHello, per-session
  goroutine with handshake state machine
- `introspector/processor.go`: per-source state (sourceState struct),
  peer event handling, reconnection logic (boot_id comparison),
  finalization callback for persistence
- `introspector/server.go`: new endpoints (/api/peers, /api/sources,
  /api/search), source query param on existing endpoints, source-scoped
  WebSocket, peer_count in WS messages, source defaulting logic
- `introspector/types.go`: PeerSummary, SourceInfo, ConnState, updated
  wsMessage
- `cmd/introspector/main.go`: --data-dir, --retention-days flags, storage
  initialization, listener setup

### Dashboard changes

- Source selector in header (dropdown when multiple sources exist)
- All API calls include `?source=<active_source_id>`
- WebSocket connects to `/api/ws?source=<id>`
- PEERS tab populated from `/api/peers` (no longer shows "unavailable")
- Search UI (input field + slot range, results displayed in slot list)
- Historical slot detail: shows flow table and chart (full buckets available)

## CLI flags

```
--socket           Ingest listen address (default: /tmp/wiretap-introspector.sock)
--listen           HTTP listen address (default: 127.0.0.1:9100)
--genesis-unix     Beacon chain genesis timestamp (default: 1606824023)
--seconds-per-slot Slot duration (default: 12)
--data-dir         Persistence directory (default: ~/.wiretap/data)
--retention-days   Slot retention period (default: 30)
```
