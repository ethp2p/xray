# Persistence, search, and multi-source implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add file-based persistence, search, multi-source support, and live peer tracking to the Wiretap backend, with corresponding dashboard changes.

**Architecture:** The ingest protocol gains a bidirectional handshake (ClientHello/ServerHello). The backend becomes a listener accepting multiple probes. The processor namespaces all state by source_id. Finalized slots are persisted as JSON files with epoch-grouped summary indices. The dashboard adds a source switcher, working peers tab, and search UI.

**Tech Stack:** Go (backend), Solid.js (dashboard), protobuf (wire protocol), JSON/JSONL (persistence)

**Spec:** `docs/superpowers/specs/2026-03-28-persistence-search-multi-source-design.md`

**Existing tests:** Unit tests in `introspector/processor_test.go`, integration tests in `itest/`. One flaky integration test (`TestIntrospectorUnixIngestHTTPAndWS`) has a WS read timeout; pre-existing, not caused by our changes.

---

## File map

### New files

| File | Responsibility |
|------|---------------|
| `introspector/codec.go` | Wire framing: type-discriminated varint-delimited protobuf read/write |
| `introspector/codec_test.go` | Codec round-trip tests |
| `introspector/peers.go` | PeerState, ConnState types and peer map operations |
| `introspector/peers_test.go` | Peer tracking unit tests |
| `introspector/sources.go` | SourceInfo type, source registry, source_id derivation from peer_id |
| `introspector/sources_test.go` | Source registry unit tests |
| `introspector/storage.go` | File-based persistence: write slot, append/load epoch index, prune, source.json |
| `introspector/storage_test.go` | Storage round-trip tests with temp directories |

### Modified files

| File | Changes |
|------|---------|
| `pb/ingest/ingest.proto` | Add `ClientHello`, `ServerHello` messages; drop `server_hello` from Envelope oneof |
| `pb/ingest/ingest.pb.go` | Regenerated from proto |
| `introspector/processor.go` | Per-source `sourceState` struct; handle `PeerUpsert`, `ConnectionUpsert`, `ConnectionClosed`; slot finalization callback; `Apply` takes `sourceID` param |
| `introspector/processor_test.go` | Update for new `Apply` signature with source_id |
| `introspector/types.go` | Add `PeerSummary`, `ConnSummary`; add `PeerCount` to `wsMessage`; drop server-side slot detail filters |
| `introspector/ingest.go` | Replace `UnixIngestClient` (dialer) with `IngestListener` (listener); handshake state machine; session exclusivity; per-connection goroutines |
| `introspector/server.go` | Add `/api/peers`, `/api/sources`, `/api/search` endpoints; `?source=` param on all endpoints; source-scoped WebSocket; slot list merges live + persisted data |
| `introspector/decode.go` | No changes (just confirming) |
| `sink_unix.go` | Rename to `sink_ingest.go`; change from listener to dialer; send `ClientHello`, read `ServerHello`; add `client_name` option |
| `options.go` | Add `WithClientName(string)` option; rename `WithUnixSocket` to `WithIngestSocket` |
| `host.go` | Pass `client_name` through to sink |
| `cmd/introspector/main.go` | New flags `--data-dir`, `--retention-days`; wire `IngestListener` + `Storage` |
| `dashboard/src/App.tsx` | Source selector, peers tab, search UI, source-scoped WS/API |

---

## Task 1: Proto changes

**Files:**
- Modify: `pb/ingest/ingest.proto`
- Regenerate: `pb/ingest/ingest.pb.go`

- [ ] **Step 1: Update the proto file**

Add `ClientHello` and `ServerHello` as top-level messages. Remove `server_hello` (field 10) from the `Envelope` oneof. Remove `ServerHello` fields `source_id` and `wait_for_attach` (those move to the new messages). Keep `SnapshotStart` and `SnapshotEnd` in the Envelope oneof.

```protobuf
// Add before Envelope:
message ClientHello {
  uint32 protocol_version = 1;
  bytes peer_id = 2;
  string client_name = 3;
  bytes boot_id = 4;
  int64 started_at_ns = 5;
}

message ServerHello {
  uint32 protocol_version = 1;
  string source_id = 2;
}
```

In the `Envelope` oneof, remove the `ServerHello server_hello = 10;` field. Keep fields 11-26 as-is.

- [ ] **Step 2: Regenerate Go code**

Run: `protoc --go_out=. --go_opt=paths=source_relative pb/ingest/ingest.proto`

If `protoc` is not installed, use the `buf` tool or check the project's codegen setup:

```bash
ls pb/ingest/buf* pb/buf* Makefile justfile 2>/dev/null
# Check for existing codegen commands
grep -r "protoc\|buf generate" justfile Makefile 2>/dev/null
```

- [ ] **Step 3: Verify compilation**

Run: `go build ./pb/ingest/...`
Expected: compiles cleanly

- [ ] **Step 4: Commit**

```bash
git add pb/ingest/
git commit -m "feat(proto): add ClientHello/ServerHello, remove server_hello from Envelope"
```

---

## Task 2: Wire framing codec

**Files:**
- Create: `introspector/codec.go`
- Create: `introspector/codec_test.go`

The wire protocol uses a single-byte type discriminator before each varint-delimited protobuf message. This codec encapsulates read/write for all three message types.

- [ ] **Step 1: Write codec tests**

Test round-trip for all three discriminator types (0x01 ClientHello, 0x02 ServerHello, 0x03 Envelope). Test error cases: unknown discriminator byte, truncated message, oversized message.

```go
// introspector/codec_test.go
package introspector

import (
    "bytes"
    "testing"

    ingestpb "github.com/ethp2p/instrument/pb/ingest"
)

func TestCodecRoundTripClientHello(t *testing.T) {
    var buf bytes.Buffer
    hello := &ingestpb.ClientHello{
        ProtocolVersion: 2,
        PeerId:          []byte("test-peer"),
        ClientName:      "prysm/v5.2.0",
    }
    if err := WriteClientHello(&buf, hello); err != nil {
        t.Fatal(err)
    }
    got, err := ReadClientHello(&buf)
    if err != nil {
        t.Fatal(err)
    }
    if got.ClientName != hello.ClientName {
        t.Fatalf("got %q, want %q", got.ClientName, hello.ClientName)
    }
}

func TestCodecRoundTripServerHello(t *testing.T) {
    var buf bytes.Buffer
    hello := &ingestpb.ServerHello{
        ProtocolVersion: 2,
        SourceId:        "16Uiu2HAm...",
    }
    if err := WriteServerHello(&buf, hello); err != nil {
        t.Fatal(err)
    }
    got, err := ReadServerHello(&buf)
    if err != nil {
        t.Fatal(err)
    }
    if got.SourceId != hello.SourceId {
        t.Fatalf("got %q, want %q", got.SourceId, hello.SourceId)
    }
}

func TestCodecRoundTripEnvelope(t *testing.T) {
    var buf bytes.Buffer
    env := &ingestpb.Envelope{
        Seq: 42,
        Payload: &ingestpb.Envelope_StringDef{
            StringDef: &ingestpb.StringDef{Id: 1, Value: "test"},
        },
    }
    if err := WriteEnvelope(&buf, env); err != nil {
        t.Fatal(err)
    }
    got, err := ReadEnvelope(&buf)
    if err != nil {
        t.Fatal(err)
    }
    if got.Seq != 42 {
        t.Fatalf("got seq %d, want 42", got.Seq)
    }
}

func TestCodecRejectsUnknownDiscriminator(t *testing.T) {
    buf := bytes.NewBuffer([]byte{0xFF, 0x00})
    _, err := ReadEnvelope(buf)
    if err == nil {
        t.Fatal("expected error for unknown discriminator")
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./introspector/ -run TestCodec -v`
Expected: FAIL (functions not defined)

- [ ] **Step 3: Implement the codec**

```go
// introspector/codec.go
package introspector

import (
    "encoding/binary"
    "errors"
    "fmt"
    "io"

    ingestpb "github.com/ethp2p/instrument/pb/ingest"
    "google.golang.org/protobuf/proto"
)

const (
    wireClientHello byte = 0x01
    wireServerHello byte = 0x02
    wireEnvelope    byte = 0x03

    maxMessageSize = 4 << 20 // 4 MiB
)

func WriteClientHello(w io.Writer, msg *ingestpb.ClientHello) error {
    return writeTyped(w, wireClientHello, msg)
}

func ReadClientHello(r io.Reader) (*ingestpb.ClientHello, error) {
    msg := &ingestpb.ClientHello{}
    if err := readTyped(r, wireClientHello, msg); err != nil {
        return nil, err
    }
    return msg, nil
}

func WriteServerHello(w io.Writer, msg *ingestpb.ServerHello) error {
    return writeTyped(w, wireServerHello, msg)
}

func ReadServerHello(r io.Reader) (*ingestpb.ServerHello, error) {
    msg := &ingestpb.ServerHello{}
    if err := readTyped(r, wireServerHello, msg); err != nil {
        return nil, err
    }
    return msg, nil
}

func WriteEnvelope(w io.Writer, msg *ingestpb.Envelope) error {
    return writeTyped(w, wireEnvelope, msg)
}

func ReadEnvelope(r io.Reader) (*ingestpb.Envelope, error) {
    msg := &ingestpb.Envelope{}
    if err := readTyped(r, wireEnvelope, msg); err != nil {
        return nil, err
    }
    return msg, nil
}

func writeTyped(w io.Writer, typ byte, msg proto.Message) error {
    data, err := proto.Marshal(msg)
    if err != nil {
        return err
    }
    if _, err := w.Write([]byte{typ}); err != nil {
        return err
    }
    var buf [binary.MaxVarintLen64]byte
    n := binary.PutUvarint(buf[:], uint64(len(data)))
    if _, err := w.Write(buf[:n]); err != nil {
        return err
    }
    _, err = w.Write(data)
    return err
}

func readTyped(r io.Reader, expectedType byte, msg proto.Message) error {
    var typBuf [1]byte
    if _, err := io.ReadFull(r, typBuf[:]); err != nil {
        return err
    }
    if typBuf[0] != expectedType {
        return fmt.Errorf("unexpected message type: got 0x%02x, want 0x%02x", typBuf[0], expectedType)
    }
    return readDelimitedInto(r, msg)
}

func readDelimitedInto(r io.Reader, msg proto.Message) error {
    var length uint64
    var shift uint
    for {
        var b [1]byte
        if _, err := io.ReadFull(r, b[:]); err != nil {
            return err
        }
        length |= uint64(b[0]&0x7f) << shift
        if b[0]&0x80 == 0 {
            break
        }
        shift += 7
        if shift >= 64 {
            return errors.New("varint overflow")
        }
    }
    if length > maxMessageSize {
        return fmt.Errorf("message too large: %d bytes", length)
    }
    data := make([]byte, length)
    if _, err := io.ReadFull(r, data); err != nil {
        return err
    }
    return proto.Unmarshal(data, msg)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./introspector/ -run TestCodec -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add introspector/codec.go introspector/codec_test.go
git commit -m "feat(introspector): add type-discriminated wire framing codec"
```

---

## Task 3: Source registry

**Files:**
- Create: `introspector/sources.go`
- Create: `introspector/sources_test.go`

- [ ] **Step 1: Write source registry tests**

Test source_id derivation from peer_id (deterministic). Test register, lookup, list, mark connected/disconnected.

```go
// introspector/sources_test.go
package introspector

import (
    "testing"
    "time"
)

func TestSourceIDDeterministic(t *testing.T) {
    peerID := []byte{0x00, 0x24, 0x08, 0x01, 0x12, 0x20, 0xAB}
    id1 := DeriveSourceID(peerID)
    id2 := DeriveSourceID(peerID)
    if id1 != id2 {
        t.Fatalf("source_id not deterministic: %q != %q", id1, id2)
    }
    if id1 == "" {
        t.Fatal("source_id should not be empty")
    }
}

func TestSourceRegistryRegisterAndList(t *testing.T) {
    reg := NewSourceRegistry()
    info := SourceInfo{
        SourceID:   "src-1",
        PeerID:     []byte("peer1"),
        ClientName: "prysm/v5.2.0",
        Connected:  true,
        ConnectedAt: time.Now(),
    }
    reg.Register(info)

    sources := reg.List()
    if len(sources) != 1 {
        t.Fatalf("expected 1 source, got %d", len(sources))
    }
    if sources[0].ClientName != "prysm/v5.2.0" {
        t.Fatalf("wrong client name: %s", sources[0].ClientName)
    }
}

func TestSourceRegistryDefaultSource(t *testing.T) {
    reg := NewSourceRegistry()

    // No sources: should return error
    _, err := reg.DefaultSourceID()
    if err == nil {
        t.Fatal("expected error with no sources")
    }

    // One source: should return it
    reg.Register(SourceInfo{SourceID: "src-1"})
    id, err := reg.DefaultSourceID()
    if err != nil {
        t.Fatal(err)
    }
    if id != "src-1" {
        t.Fatalf("expected src-1, got %s", id)
    }

    // Two sources: should return error
    reg.Register(SourceInfo{SourceID: "src-2"})
    _, err = reg.DefaultSourceID()
    if err == nil {
        t.Fatal("expected error with multiple sources")
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./introspector/ -run TestSource -v`
Expected: FAIL

- [ ] **Step 3: Implement source registry**

```go
// introspector/sources.go
package introspector

import (
    "errors"
    "sync"
    "time"

    "github.com/libp2p/go-libp2p/core/peer"
)

type SourceInfo struct {
    SourceID    string    `json:"source_id"`
    PeerID      []byte    `json:"peer_id"`
    ClientName  string    `json:"client_name"`
    BootID      []byte    `json:"boot_id,omitempty"`
    StartedAtNs int64     `json:"started_at_ns,omitempty"`
    ConnectedAt time.Time `json:"connected_at,omitempty"`
    Connected   bool      `json:"connected"`
}

// DeriveSourceID produces the standard libp2p peer ID string from raw bytes.
// This matches what peer.ID.String() returns on the probe side.
func DeriveSourceID(peerID []byte) string {
    return peer.ID(peerID).String()
}

type SourceRegistry struct {
    mu      sync.RWMutex
    sources map[string]*SourceInfo
}

func NewSourceRegistry() *SourceRegistry {
    return &SourceRegistry{
        sources: make(map[string]*SourceInfo),
    }
}

func (r *SourceRegistry) Register(info SourceInfo) {
    r.mu.Lock()
    defer r.mu.Unlock()
    cp := info
    r.sources[info.SourceID] = &cp
}

func (r *SourceRegistry) Get(sourceID string) (SourceInfo, bool) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    s, ok := r.sources[sourceID]
    if !ok {
        return SourceInfo{}, false
    }
    return *s, true
}

func (r *SourceRegistry) SetConnected(sourceID string, connected bool) {
    r.mu.Lock()
    defer r.mu.Unlock()
    if s, ok := r.sources[sourceID]; ok {
        s.Connected = connected
    }
}

func (r *SourceRegistry) List() []SourceInfo {
    r.mu.RLock()
    defer r.mu.RUnlock()
    list := make([]SourceInfo, 0, len(r.sources))
    for _, s := range r.sources {
        list = append(list, *s)
    }
    return list
}

func (r *SourceRegistry) DefaultSourceID() (string, error) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    if len(r.sources) == 1 {
        for id := range r.sources {
            return id, nil
        }
    }
    if len(r.sources) == 0 {
        return "", errors.New("no sources available")
    }
    return "", errors.New("multiple sources available, specify ?source=<id>")
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./introspector/ -run TestSource -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add introspector/sources.go introspector/sources_test.go
git commit -m "feat(introspector): add source registry with deterministic source_id"
```

---

## Task 4: Peer tracking types and operations

**Files:**
- Create: `introspector/peers.go`
- Create: `introspector/peers_test.go`

- [ ] **Step 1: Write peer tracking tests**

Test: add peer, add connections to peer, close connection, peer removed when last connection closes.

```go
// introspector/peers_test.go
package introspector

import "testing"

func TestPeerMapAddAndRemove(t *testing.T) {
    pm := NewPeerMap()

    pm.UpsertPeer(1, []byte("peer-abc"))
    pm.UpsertConnection(1, ConnState{
        PeerAlias:  1,
        RemoteAddr: "/ip4/1.2.3.4/tcp/9000",
        Direction:  "inbound",
        OpenedAtNs: 1000,
    })

    peers := pm.ListPeers()
    if len(peers) != 1 {
        t.Fatalf("expected 1 peer, got %d", len(peers))
    }
    if len(peers[0].Connections) != 1 {
        t.Fatalf("expected 1 connection, got %d", len(peers[0].Connections))
    }

    // Close connection -> peer removed (no remaining connections)
    pm.CloseConnection(1)
    peers = pm.ListPeers()
    if len(peers) != 0 {
        t.Fatalf("expected 0 peers after last connection closed, got %d", len(peers))
    }
}

func TestPeerMapMultipleConnections(t *testing.T) {
    pm := NewPeerMap()
    pm.UpsertPeer(1, []byte("peer-abc"))
    pm.UpsertConnection(10, ConnState{PeerAlias: 1, RemoteAddr: "addr1", OpenedAtNs: 1000})
    pm.UpsertConnection(11, ConnState{PeerAlias: 1, RemoteAddr: "addr2", OpenedAtNs: 2000})

    peers := pm.ListPeers()
    if len(peers[0].Connections) != 2 {
        t.Fatalf("expected 2 connections, got %d", len(peers[0].Connections))
    }

    pm.CloseConnection(10)
    peers = pm.ListPeers()
    if len(peers) != 1 {
        t.Fatal("peer should still exist with one connection")
    }
    if len(peers[0].Connections) != 1 {
        t.Fatalf("expected 1 connection remaining, got %d", len(peers[0].Connections))
    }
}
```

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./introspector/ -run TestPeerMap -v`

- [ ] **Step 3: Implement peer tracking**

```go
// introspector/peers.go
package introspector

import "github.com/libp2p/go-libp2p/core/peer"

type ConnState struct {
    PeerAlias  uint64 `json:"-"`
    RemoteAddr string `json:"remote_addr"`
    LocalAddr  string `json:"local_addr,omitempty"`
    Direction  string `json:"direction"`
    Transport  string `json:"transport,omitempty"`
    Security   string `json:"security,omitempty"`
    Muxer      string `json:"muxer,omitempty"`
    OpenedAtNs int64  `json:"opened_at_ns"`
}

type PeerState struct {
    PeerID      []byte
    Connections map[uint64]*ConnState
    FirstSeenNs int64
    LastSeenNs  int64
}

type PeerSummary struct {
    PeerID      string      `json:"peer_id"`
    Connections []ConnState `json:"connections"`
    FirstSeenNs int64       `json:"first_seen_ns"`
    LastSeenNs  int64       `json:"last_seen_ns"`
}

type PeerMap struct {
    peers map[uint64]*PeerState  // peer_alias -> PeerState
    conns map[uint64]uint64      // conn_alias -> peer_alias
}

func NewPeerMap() *PeerMap {
    return &PeerMap{
        peers: make(map[uint64]*PeerState),
        conns: make(map[uint64]uint64),
    }
}

func (m *PeerMap) UpsertPeer(alias uint64, peerID []byte) {
    ps := m.peers[alias]
    if ps == nil {
        ps = &PeerState{
            PeerID:      append([]byte(nil), peerID...),
            Connections: make(map[uint64]*ConnState),
        }
        m.peers[alias] = ps
    }
    ps.PeerID = append(ps.PeerID[:0], peerID...)
}

func (m *PeerMap) UpsertConnection(connAlias uint64, cs ConnState) {
    peerAlias := cs.PeerAlias
    ps := m.peers[peerAlias]
    if ps == nil {
        return
    }
    cp := cs
    ps.Connections[connAlias] = &cp
    m.conns[connAlias] = peerAlias

    now := cs.OpenedAtNs
    if ps.FirstSeenNs == 0 || now < ps.FirstSeenNs {
        ps.FirstSeenNs = now
    }
    if now > ps.LastSeenNs {
        ps.LastSeenNs = now
    }
}

func (m *PeerMap) CloseConnection(connAlias uint64) {
    peerAlias, ok := m.conns[connAlias]
    if !ok {
        return
    }
    delete(m.conns, connAlias)

    ps := m.peers[peerAlias]
    if ps == nil {
        return
    }
    delete(ps.Connections, connAlias)
    if len(ps.Connections) == 0 {
        delete(m.peers, peerAlias)
    }
}

func (m *PeerMap) ListPeers() []PeerSummary {
    result := make([]PeerSummary, 0, len(m.peers))
    for _, ps := range m.peers {
        summary := PeerSummary{
            PeerID:      peer.ID(ps.PeerID).String(),
            FirstSeenNs: ps.FirstSeenNs,
            LastSeenNs:  ps.LastSeenNs,
        }
        for _, conn := range ps.Connections {
            summary.Connections = append(summary.Connections, *conn)
        }
        result = append(result, summary)
    }
    return result
}

func (m *PeerMap) Count() int {
    return len(m.peers)
}

func (m *PeerMap) Clear() {
    m.peers = make(map[uint64]*PeerState)
    m.conns = make(map[uint64]uint64)
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./introspector/ -run TestPeerMap -v`

- [ ] **Step 5: Commit**

```bash
git add introspector/peers.go introspector/peers_test.go
git commit -m "feat(introspector): add peer tracking with connection lifecycle"
```

---

## Task 5: Processor refactor for multi-source and peer tracking

**Files:**
- Modify: `introspector/processor.go`
- Modify: `introspector/processor_test.go`
- Modify: `introspector/types.go`

This is the largest task. The processor gains per-source state isolation and handles peer/connection events.

- [ ] **Step 1: Update types.go**

Add `PeerCount` to `wsMessage`. Drop the server-side filter params from `SlotDetail` (the `SlotDetail` method will no longer accept filter params; see spec read path change).

In `types.go`, add `PeerCount *int` to `wsMessage`:

```go
type wsMessage struct {
    Type      string        `json:"type"`
    Slot      *SlotSummary  `json:"slot,omitempty"`
    Slots     []SlotSummary `json:"slots,omitempty"`
    Current   uint64        `json:"current_slot,omitempty"`
    PeerCount *int          `json:"peer_count,omitempty"`
}
```

- [ ] **Step 2: Refactor Processor to per-source state**

Replace the flat maps with a `sourceState` struct. Change `Apply` to `ApplyForSource(sourceID string, event *ingestpb.Envelope)`. Add `ResetSourceAliases(sourceID string)` for reconnection. Add peer event handling.

Key changes to `processor.go`:

```go
type sourceState struct {
    strings map[uint32]string
    streams map[uint64]*streamState
    peers   *PeerMap
    slots   map[uint64]*slotAggregate
}

type Processor struct {
    mu    sync.RWMutex
    clock eth.SlotClock
    sources map[string]*sourceState
    onUpdate func(string, SlotSummary, uint64)  // sourceID, summary, currentSlot
    onFinalize func(string, SlotDetail)           // sourceID, detail
}
```

`ApplyForSource` replaces `Apply`. All internal methods (`handleStreamUpsert`, etc.) receive `*sourceState` instead of reaching into `p.strings`, `p.streams` etc.

- [ ] **Step 3: Handle peer events in processor**

In `ApplyForSource`, add cases for `PeerUpsert`, `ConnectionUpsert`, `ConnectionClosed`:

```go
case *ingestpb.Envelope_PeerUpsert:
    src.peers.UpsertPeer(payload.PeerUpsert.PeerAlias, payload.PeerUpsert.PeerId)
case *ingestpb.Envelope_ConnectionUpsert:
    cu := payload.ConnectionUpsert
    src.peers.UpsertConnection(cu.ConnAlias, ConnState{
        PeerAlias:  cu.PeerAlias,
        RemoteAddr: cu.RemoteAddr,
        LocalAddr:  cu.LocalAddr,
        Direction:  dirString(cu.Direction),
        Transport:  src.strings[cu.TransportId],
        Security:   src.strings[cu.SecurityId],
        Muxer:      src.strings[cu.MuxerId],
        OpenedAtNs: cu.OpenedAtNs,
    })
case *ingestpb.Envelope_ConnectionClosed:
    src.peers.CloseConnection(payload.ConnectionClosed.ConnAlias)
```

- [ ] **Step 4: Add slot finalization detection**

Track a `lastSlot uint64` per source. In `handleStreamChunk`, after computing `currentSlot`, compare against `lastSlot`. When `currentSlot` advances past `lastSlot`, the previous slot is finalized. Build the `SlotDetail` for the finalized slot and send it to `onFinalize`. This avoids iterating all slots on every chunk.

```go
// In sourceState:
//   lastSlot uint64

// In handleStreamChunk, after computing currentSlot:
if src.lastSlot > 0 && currentSlot > src.lastSlot && src.lastSlot != currentSlot {
    if agg := src.slots[src.lastSlot]; agg != nil {
        detail := p.buildSlotDetailLocked(src, src.lastSlot)
        if p.onFinalize != nil {
            // Send to persistence channel (non-blocking)
            p.onFinalize(sourceID, detail)
        }
    }
}
src.lastSlot = currentSlot

// Evict old slots beyond 256
if len(src.slots) > 256 {
    // Find and delete the oldest
    var oldest uint64
    for slot := range src.slots {
        if oldest == 0 || slot < oldest {
            oldest = slot
        }
    }
    delete(src.slots, oldest)
}
```

Add a test that verifies finalization fires when the slot advances and that eviction kicks in after 256 slots.

- [ ] **Step 5: Update NewServer signature**

`NewServer` needs the source registry and storage to resolve sources and serve historical data. Update the signature now so downstream tasks can compile:

```go
func NewServer(processor *Processor, registry *SourceRegistry, storage *Storage) *Server
```

Store `registry` and `storage` as fields on `Server`. The `broadcastUpdate` method signature changes to `func(sourceID string, summary SlotSummary, current uint64)` to match the processor's new `onUpdate`. For now, the method ignores `sourceID` and broadcasts to all clients (source scoping is added in Task 9).

- [ ] **Step 6: Update SlotDetail to not filter**

Change `SlotDetail` method signature to `SlotDetail(sourceID string, slot uint64) (SlotDetail, bool)`. Remove the `protocol`, `topic`, `messageKind` filter parameters. Return the full unfiltered breakdown.

- [ ] **Step 7: Update existing test**

Update `TestProcessorTracksSlotTrafficAndBreakdown` to use `ApplyForSource("test-source", event)` and `SlotDetail("test-source", ref.Slot)`. Update `NewServer` call in tests to pass `registry` and `storage` (can be `nil` for unit tests that don't exercise them).

- [ ] **Step 8: Run tests**

Run: `go test ./introspector/ -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add introspector/processor.go introspector/processor_test.go introspector/types.go
git commit -m "refactor(introspector): per-source state isolation, peer tracking, slot finalization"
```

---

## Task 6: File-based persistence (storage layer)

**Files:**
- Create: `introspector/storage.go`
- Create: `introspector/storage_test.go`

- [ ] **Step 1: Write storage tests**

Test: write a slot detail + summary, read it back. Load epoch index. Duplicate detection. Truncated JSONL handling. Source metadata write/read.

```go
// Key test cases:
func TestStorageWriteAndReadSlot(t *testing.T)
func TestStorageEpochIndex(t *testing.T)
func TestStorageDuplicateSlotSkipped(t *testing.T)
func TestStorageTruncatedJSONLRecovery(t *testing.T)
func TestStorageSourceMetadata(t *testing.T)
func TestStorageLoadSummaryIndex(t *testing.T)
```

Each test creates a `t.TempDir()` and operates on it.

- [ ] **Step 2: Run tests, verify failure**

Run: `go test ./introspector/ -run TestStorage -v`

- [ ] **Step 3: Implement storage**

The `Storage` struct manages a data directory. Key methods:

```go
type Storage struct {
    dataDir string
    mu      sync.Mutex
    indices map[string][]SlotSummary  // sourceID -> sorted summaries
}

func NewStorage(dataDir string) (*Storage, error)
func (s *Storage) WriteSlot(sourceID string, detail SlotDetail) error
func (s *Storage) ReadSlot(sourceID string, slot uint64) ([]byte, error)
func (s *Storage) ListSummaries(sourceID string, limit int) []SlotSummary
func (s *Storage) SearchSlots(sourceID string, fromSlot, toSlot uint64, limit int) []SlotSummary
func (s *Storage) WriteSourceMeta(info SourceInfo) error
func (s *Storage) LoadSourceMetas() ([]SourceInfo, error)
func (s *Storage) Prune(retentionDays int, slotsPerEpoch uint64, secondsPerSlot uint64) error
```

`WriteSlot`: serializes detail to JSON, writes to temp file, renames into place. Appends summary line to epoch JSONL (with duplicate check). Inserts into in-memory index.

`ReadSlot`: returns raw JSON bytes (for passthrough to HTTP response).

`LoadSummaryIndex`: called on startup, reads all epoch JSONL files for a source into the in-memory index.

- [ ] **Step 4: Run tests, verify pass**

Run: `go test ./introspector/ -run TestStorage -v`

- [ ] **Step 5: Commit**

```bash
git add introspector/storage.go introspector/storage_test.go
git commit -m "feat(introspector): add file-based persistence with epoch-grouped indices"
```

---

## Task 7: Ingest listener (backend side)

**Files:**
- Modify: `introspector/ingest.go`

Replace `UnixIngestClient` (dials probe) with `IngestListener` (listens for probes).

- [ ] **Step 1: Implement IngestListener**

```go
type IngestListener struct {
    processor *Processor
    registry  *SourceRegistry
    storage   *Storage
    listener  net.Listener
    sessions  map[string]context.CancelFunc  // sourceID -> cancel
    mu        sync.Mutex
}

func NewIngestListener(processor *Processor, registry *SourceRegistry, storage *Storage) *IngestListener

func (l *IngestListener) ListenAndServe(ctx context.Context, address string) error
func (l *IngestListener) handleConnection(ctx context.Context, conn net.Conn)
```

`handleConnection` runs in a goroutine per connection (with a `context.Context` for shutdown):
1. Read `ClientHello` via codec
2. Validate `protocol_version`; close connection if unsupported
3. Derive `source_id` from `peer_id` using `peer.ID(hello.PeerId).String()`
4. Cancel any existing session for this source_id (session exclusivity).
   Store both the cancel func and the net.Conn so the old connection can
   be forcibly closed (a blocked `ReadEnvelope` on a net.Conn won't exit
   from context cancellation alone):
   ```go
   type activeSession struct {
       cancel func()
       conn   net.Conn
   }
   // sessions map[string]*activeSession

   l.mu.Lock()
   if old, ok := l.sessions[sourceID]; ok {
       old.cancel()
       old.conn.Close() // unblocks any pending Read
   }
   ctx, cancel := context.WithCancel(l.ctx)
   l.sessions[sourceID] = &activeSession{cancel: cancel, conn: conn}
   l.mu.Unlock()
   ```
5. Send `ServerHello` with assigned source_id
6. Build `SourceInfo` from ClientHello fields and register in registry
7. Persist source metadata: `storage.WriteSourceMeta(info)`
8. Reset processor alias state: `processor.ResetSourceAliases(sourceID)`
9. Read Envelope messages in a loop (checking `ctx.Done()`), call `processor.ApplyForSource`
10. On disconnect: `registry.SetConnected(sourceID, false)`, remove from sessions map

- [ ] **Step 2: Keep the old `UnixIngestClient` temporarily**

Don't delete it yet. The integration tests use it. We'll update the tests after the probe-side changes (Task 8).

- [ ] **Step 3: Verify existing tests still pass**

Run: `go test ./introspector/ -v`
Expected: PASS (new code is additive, old code not yet deleted)

- [ ] **Step 4: Commit**

```bash
git add introspector/ingest.go
git commit -m "feat(introspector): add IngestListener with handshake and session exclusivity"
```

---

## Task 8: Probe-side dialer

**Files:**
- Rename: `sink_unix.go` -> `sink_ingest.go`
- Modify: `options.go`
- Modify: `host.go`

- [ ] **Step 1: Add WithClientName option**

In `options.go`, add `clientName string` to `config` and `WithClientName(name string) Option`.

- [ ] **Step 2: Refactor SinkUnix to SinkIngest**

Rename the file and type. Change from listener to dialer:
- Remove `acceptLoop`, `listener`, `clients` map
- Add `connect()` method that dials the backend socket
- On connect: send `ClientHello` (with peer_id, client_name, boot_id), read `ServerHello`, store source_id
- Then send the snapshot (SnapshotStart, StringDefs, PeerUpserts, etc., SnapshotEnd)
- Then stream live events
- All writes use the new codec (type discriminator + varint)

The `WaitForAttach` behavior changes: it now means "block until connected to backend" (the probe is the dialer, so it retries until the backend is available).

- [ ] **Step 3: Update host.go**

Pass `cfg.clientName` to `NewSinkIngest`.

- [ ] **Step 4: Update integration tests**

Update `startIntrospectorFixture` in `itest/introspector_test.go`:
- Replace `NewUnixIngestClient` with `NewIngestListener`
- The fixture now starts the listener, and the probe dials in
- Update `instrument.Wrap` calls to use the new option names

- [ ] **Step 5: Run integration tests**

Run: `go test ./itest/ -run TestIntrospector -v -timeout 60s`

- [ ] **Step 6: Delete old UnixIngestClient code**

Remove the old `runOnce()` dialer method from `ingest.go` and the now-unused `readDelimited`/`writeDelimited` functions (replaced by codec).

- [ ] **Step 7: Commit**

```bash
git add sink_ingest.go options.go host.go introspector/ingest.go itest/
git rm sink_unix.go
git commit -m "feat: invert connection topology, probe dials backend"
```

---

## Task 9: Server API updates

**Files:**
- Modify: `introspector/server.go`
- Modify: `cmd/introspector/main.go`

- [ ] **Step 1: Add source resolution helper**

```go
func (s *Server) resolveSource(r *http.Request) (string, error)
```

Reads `?source=` from query. If missing, uses `registry.DefaultSourceID()`. Returns 400 error if ambiguous.

- [ ] **Step 2: Update existing endpoints**

- `handleSlots`: use `resolveSource`, merge live + persisted summaries, de-duplicate by slot number (live wins)
- `handleSlotDetail`: use `resolveSource`, check in-memory first, fall back to `storage.ReadSlot` (returns raw JSON bytes, write directly to response). Drop filter params.
- `handleWS`: accept `?source=` param, only broadcast updates for that source.
  Change client tracking from `map[*websocket.Conn]struct{}` to:
  ```go
  type wsClient struct {
      conn     *websocket.Conn
      sourceID string
  }
  // clients map[*websocket.Conn]*wsClient
  ```
  In `broadcastUpdate(sourceID string, summary SlotSummary, current uint64)`,
  skip clients whose `sourceID` does not match. Update `NewServer` to pass
  `registry` and `storage` so the server can resolve sources.

- [ ] **Step 3: Add new endpoints**

- `GET /api/sources`: return `registry.List()`
- `GET /api/peers`: resolve source, return `processor.ListPeers(sourceID)`
- `GET /api/search`: resolve source, call `storage.SearchSlots`

Register all routes in `Handler()`.

- [ ] **Step 4: Update main.go**

Add `--data-dir` and `--retention-days` flags. Create `Storage`, `SourceRegistry`, `IngestListener`. Load historical source metas and summary indices on startup. Start retention pruning goroutine.

```go
storage, err := introspector.NewStorage(*dataDir)
registry := introspector.NewSourceRegistry()

// Load historical sources
metas, _ := storage.LoadSourceMetas()
for _, meta := range metas {
    registry.Register(meta)
    storage.LoadSummaryIndex(meta.SourceID)
}

// Wire finalization: processor -> buffered channel -> storage goroutine
finalizeCh := make(chan introspector.FinalizedSlot, 64)
// Blocking send: persistence is the only path into historical storage,
// so we must not drop finalized slots. Backpressure is acceptable here
// because finalization happens at most once per 12 seconds per source,
// and the persistence goroutine writes at ~1ms per slot. A channel of
// 64 gives >12 minutes of buffer before blocking the ingest goroutine.
processor.SetOnFinalize(func(sourceID string, detail introspector.SlotDetail) {
    finalizeCh <- introspector.FinalizedSlot{SourceID: sourceID, Detail: detail}
})

// Single-writer persistence goroutine
go func() {
    for item := range finalizeCh {
        if err := storage.WriteSlot(item.SourceID, item.Detail); err != nil {
            log.Printf("persist slot %d: %v", item.Detail.Summary.Slot, err)
        }
    }
}()

// Retention pruning: daily
go func() {
    ticker := time.NewTicker(24 * time.Hour)
    defer ticker.Stop()
    for range ticker.C {
        if err := storage.Prune(*retentionDays, 32, *secondsPerSlot); err != nil {
            log.Printf("retention prune: %v", err)
        }
    }
}()

listener := introspector.NewIngestListener(processor, registry, storage)
```

Define `FinalizedSlot` in `types.go`:

```go
type FinalizedSlot struct {
    SourceID string
    Detail   SlotDetail
}
```

- [ ] **Step 5: Run full test suite**

Run: `go test ./... -timeout 120s`

- [ ] **Step 6: Commit**

```bash
git add introspector/server.go cmd/introspector/main.go
git commit -m "feat(introspector): add source/peers/search endpoints, persistence wiring"
```

---

## Task 10: Dashboard updates

**Files:**
- Modify: `dashboard/src/App.tsx`

This task covers all frontend changes. The dashboard is a single-file Solid.js app (~1800 lines).

- [ ] **Step 1: Add source state and fetching**

Add a `sources` signal fetched from `/api/sources`. Add `activeSource` signal. When sources are loaded, auto-select the only source (or first connected). All API calls append `?source=<activeSource()>`.

- [ ] **Step 2: Add source selector to header**

When multiple sources exist, render a dropdown in the header bar (between the title and the theme toggle). Show `client_name` as the label. When only one source exists, show it as static text.

On source change: update `activeSource`, close and reopen WebSocket with `?source=` param, re-fetch slot list.

- [ ] **Step 3: Update WebSocket connection**

Modify the WebSocket URL to include `?source=<activeSource()>`. On source change, disconnect and reconnect.

- [ ] **Step 4: Update PEERS tab**

Replace the "Peer data unavailable in live mode" message with a fetch from `/api/peers?source=<id>`. Render the peer list with columns: peer_id (truncated), connections count, direction, remote_addr, first_seen.

Also display `peer_count` from WS `slot_batch` messages in the header status bar.

- [ ] **Step 5: Add search UI**

Add a search panel (toggle with `/` key or a search icon). Input fields: slot range (from/to). Submitting queries `/api/search?source=<id>&from_slot=N&to_slot=N`. Results replace the slot list temporarily. Clicking a search result fetches the slot detail and displays it. Press Escape to return to live view.

- [ ] **Step 6: Update API call URLs**

Audit all `fetch()` calls and WebSocket URLs. Ensure every one includes the `?source=` parameter.

- [ ] **Step 7: Test manually**

Start the backend with `go run ./cmd/introspector/`, start the dashboard with `cd dashboard && bun run dev`, verify:
- Source selector appears (may show one source)
- PEERS tab shows live peer data
- Slot list loads with source param
- Search works (if there's persisted data)

- [ ] **Step 8: Build check**

Run: `cd dashboard && bun run build`
Expected: builds without errors

- [ ] **Step 9: Commit**

```bash
git add dashboard/src/App.tsx
git commit -m "feat(dashboard): add source selector, peers tab, search UI"
```

---

## Task 11: Final integration test

**Files:**
- Modify: `itest/introspector_test.go`

- [ ] **Step 1: Add multi-source integration test**

Test that two probes can connect simultaneously, each gets its own source_id, and slot data is isolated.

- [ ] **Step 2: Add persistence integration test**

Test that a finalized slot is written to disk and can be retrieved via the search API after the in-memory slot is evicted.

- [ ] **Step 3: Add peers integration test**

Test that `/api/peers` returns connected peers after a probe connects and exchanges streams.

- [ ] **Step 4: Run full test suite**

Run: `go test ./... -timeout 120s`

- [ ] **Step 5: Commit**

```bash
git add itest/
git commit -m "test: add multi-source, persistence, and peers integration tests"
```
