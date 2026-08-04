package xray

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	xraypb "github.com/ethp2p/xray/proto/xray"
)

// wrappedConn tracks the instrumentation state on a connection.
type wrappedConn struct {
	network.Conn
	connID    uint32
	peerAlias uint64
}

type trackedPeer struct {
	alias  uint64
	peerID peer.ID
}

// wrappedStream intercepts Read/Write to emit StreamChunk envelopes and forward
// data to the async decode worker.
type wrappedStream struct {
	network.Stream

	net        *wrappedNetwork
	wconn      *wrappedConn
	streamID   uint32
	protocol   string
	emitter    *Emitter
	worker     *decodeWorker
	initDecode func(streamID uint32, protocol string) (StreamDecoder, []OnMessage)
	decoder    StreamDecoder
	handlers   []OnMessage
	failed     bool
}

func (s *wrappedStream) SetProtocol(id protocol.ID) error {
	if err := s.Stream.SetProtocol(id); err != nil {
		return err
	}
	s.protocol = string(id)
	if s.initDecode != nil {
		s.decoder, s.handlers = s.initDecode(s.streamID, s.protocol)
	}
	return nil
}

func (s *wrappedStream) Conn() network.Conn {
	return s.wconn
}

func (s *wrappedStream) Read(p []byte) (int, error) {
	n, err := s.Stream.Read(p)
	if n > 0 {
		s.emitChunk(DirectionIn, p[:n])
	}
	return n, err
}

func (s *wrappedStream) Write(p []byte) (int, error) {
	n, err := s.Stream.Write(p)
	if n > 0 {
		s.emitChunk(DirectionOut, p[:n])
	}
	return n, err
}

// emitChunk copies the bytes once and shares the copy between the Envelope
// payload (frozen once Emit dispatches) and the async decode worker. The
// envelope is read-only after Emit returns; the worker only reads.
func (s *wrappedStream) emitChunk(dir Direction, data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)
	s.emitter.Emit(&xraypb.Envelope{
		Payload: &xraypb.Envelope_StreamChunk{
			StreamChunk: &xraypb.StreamChunk{
				StreamAlias: uint64(s.streamID),
				Direction:   directionToIngest(dir),
				Data:        cp,
			},
		},
	})
	if s.decoder != nil {
		s.worker.sendShared(s, dir, cp)
	}
}

func (s *wrappedStream) Close() error {
	s.emitClosed(xraypb.CloseReason_CLOSE_REASON_CLOSE)
	return s.Stream.Close()
}

func (s *wrappedStream) Reset() error {
	s.emitClosed(xraypb.CloseReason_CLOSE_REASON_RESET)
	return s.Stream.Reset()
}

func (s *wrappedStream) emitClosed(reason xraypb.CloseReason) {
	s.net.removeStream(s.streamID)
	s.emitter.Emit(&xraypb.Envelope{
		Payload: &xraypb.Envelope_StreamClosed{
			StreamClosed: &xraypb.StreamClosed{
				StreamAlias: uint64(s.streamID),
				ClosedAtNs:  time.Now().UnixNano(),
				Reason:      reason,
			},
		},
	})
}

// wrappedNetwork wraps network.Network to intercept stream creation.
type wrappedNetwork struct {
	network.Network
	emitter    *Emitter
	strings    *stringInterner
	worker     *decodeWorker
	initDecode func(streamID uint32, protocol string) (StreamDecoder, []OnMessage)

	mu                sync.RWMutex
	peers             map[peer.ID]*trackedPeer
	conns             map[string]*wrappedConn
	connByID          map[uint32]*wrappedConn
	connectionUpserts map[uint32]*xraypb.ConnectionUpsert
	streamUpserts     map[uint32]*xraypb.StreamUpsert
}

// connectionUpsertFor builds a ConnectionUpsert from a libp2p connection,
// allocating a peer alias for the remote peer if it is new. Returns the
// wrappedConn (created or pre-existing), the upsert, and whether the
// connection was newly tracked.
//
// String interning happens before n.mu is taken: Intern emits a StringDef
// envelope on cache miss, which fans out to sinks (e.g. SinkFile.Write takes
// SinkFile.mu). The periodic snapshot loop on SinkFile takes those locks in
// the opposite order — SinkFile.mu then n.mu via Emitter.Snapshot — so
// holding n.mu across Intern would deadlock under contention.
func (n *wrappedNetwork) connectionUpsertFor(conn network.Conn, connID uint32, openedAtNs int64) (*wrappedConn, *xraypb.ConnectionUpsert, bool) {
	state := conn.ConnState()
	transportID := n.strings.Intern(state.Transport)
	securityID := n.strings.Intern(string(state.Security))
	muxerID := n.strings.Intern(string(state.StreamMultiplexer))

	n.mu.Lock()
	defer n.mu.Unlock()

	if existing := n.conns[conn.ID()]; existing != nil {
		return existing, n.connectionUpserts[existing.connID], false
	}

	peerInfo := n.peers[conn.RemotePeer()]
	if peerInfo == nil {
		peerInfo = &trackedPeer{
			alias:  n.emitter.NextPeerAlias(),
			peerID: conn.RemotePeer(),
		}
		n.peers[conn.RemotePeer()] = peerInfo
	}

	upsert := &xraypb.ConnectionUpsert{
		ConnAlias:   uint64(connID),
		PeerAlias:   peerInfo.alias,
		RemoteAddr:  conn.RemoteMultiaddr().String(),
		LocalAddr:   conn.LocalMultiaddr().String(),
		Direction:   directionToIngest(directionFromNetwork(conn.Stat().Direction)),
		TransportId: transportID,
		SecurityId:  securityID,
		MuxerId:     muxerID,
		OpenedAtNs:  openedAtNs,
	}

	wc := &wrappedConn{
		Conn:      conn,
		connID:    connID,
		peerAlias: peerInfo.alias,
	}
	n.conns[conn.ID()] = wc
	n.connByID[connID] = wc
	n.connectionUpserts[connID] = upsert
	return wc, upsert, true
}

func (n *wrappedNetwork) peerUpsertFor(alias uint64, id peer.ID) *xraypb.PeerUpsert {
	return &xraypb.PeerUpsert{
		PeerAlias: alias,
		PeerId:    []byte(id),
	}
}

func (n *wrappedNetwork) getConn(conn network.Conn) *wrappedConn {
	n.mu.RLock()
	wc := n.conns[conn.ID()]
	n.mu.RUnlock()
	return wc
}

func (n *wrappedNetwork) removeAndGetConn(conn network.Conn) *wrappedConn {
	id := conn.ID()
	n.mu.Lock()
	wc := n.conns[id]
	delete(n.conns, id)
	if wc != nil {
		delete(n.connByID, wc.connID)
		delete(n.connectionUpserts, wc.connID)
	}
	n.mu.Unlock()
	return wc
}

// removeStreamsForConn removes every tracked stream belonging to the given
// connection alias and returns their stream aliases. Caller is responsible
// for emitting StreamClosed envelopes for them — the conn is going away and
// libp2p won't deliver per-stream Close/Reset for these.
func (n *wrappedNetwork) removeStreamsForConn(connAlias uint32) []uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	var aliases []uint64
	for id, u := range n.streamUpserts {
		if u.ConnAlias == uint64(connAlias) {
			aliases = append(aliases, u.StreamAlias)
			delete(n.streamUpserts, id)
		}
	}
	return aliases
}

func (n *wrappedNetwork) addStreamUpsert(u *xraypb.StreamUpsert) {
	n.mu.Lock()
	n.streamUpserts[uint32(u.StreamAlias)] = u
	n.mu.Unlock()
}

func (n *wrappedNetwork) removeStream(streamID uint32) {
	n.mu.Lock()
	delete(n.streamUpserts, streamID)
	n.mu.Unlock()
}

// snapshot returns the current state as ingest envelope payloads, sorted by
// alias so replays are byte-stable across runs (file-based golden tests, debug
// diffs). Suitable for bringing a fresh consumer up to date.
func (n *wrappedNetwork) snapshot() ([]*xraypb.PeerUpsert, []*xraypb.ConnectionUpsert, []*xraypb.StreamUpsert) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	peers := make([]*xraypb.PeerUpsert, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, n.peerUpsertFor(p.alias, p.peerID))
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].PeerAlias < peers[j].PeerAlias })

	connections := make([]*xraypb.ConnectionUpsert, 0, len(n.connectionUpserts))
	for _, c := range n.connectionUpserts {
		connections = append(connections, c)
	}
	sort.Slice(connections, func(i, j int) bool { return connections[i].ConnAlias < connections[j].ConnAlias })

	streams := make([]*xraypb.StreamUpsert, 0, len(n.streamUpserts))
	for _, s := range n.streamUpserts {
		streams = append(streams, s)
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i].StreamAlias < streams[j].StreamAlias })

	return peers, connections, streams
}

func (n *wrappedNetwork) NewStream(ctx context.Context, p peer.ID) (network.Stream, error) {
	s, err := n.Network.NewStream(ctx, p)
	if err != nil {
		return nil, err
	}
	return n.wrapStream(s), nil
}

func (n *wrappedNetwork) wrapStream(s network.Stream) *wrappedStream {
	streamID := n.emitter.NextStreamID()
	proto := string(s.Protocol())

	wc := n.getConn(s.Conn())
	if wc == nil {
		openedAt := time.Now().UnixNano()
		connID := n.emitter.NextConnID()
		var upsert *xraypb.ConnectionUpsert
		var created bool
		wc, upsert, created = n.connectionUpsertFor(s.Conn(), connID, openedAt)
		if created {
			n.emitter.Emit(&xraypb.Envelope{
				Payload: &xraypb.Envelope_PeerUpsert{
					PeerUpsert: n.peerUpsertFor(wc.peerAlias, s.Conn().RemotePeer()),
				},
			})
			n.emitter.Emit(&xraypb.Envelope{
				Payload: &xraypb.Envelope_ConnectionUpsert{
					ConnectionUpsert: upsert,
				},
			})
		}
	}

	ws := &wrappedStream{
		Stream:     s,
		net:        n,
		wconn:      wc,
		streamID:   streamID,
		protocol:   proto,
		emitter:    n.emitter,
		worker:     n.worker,
		initDecode: n.initDecode,
	}
	if proto != "" && n.initDecode != nil {
		ws.decoder, ws.handlers = n.initDecode(streamID, proto)
	}

	streamUpsert := &xraypb.StreamUpsert{
		StreamAlias: uint64(streamID),
		ConnAlias:   uint64(wc.connID),
		Direction:   directionToIngest(directionFromNetwork(s.Stat().Direction)),
		ProtocolId:  n.strings.Intern(proto),
		OpenedAtNs:  time.Now().UnixNano(),
	}
	n.addStreamUpsert(streamUpsert)
	n.emitter.Emit(&xraypb.Envelope{
		Payload: &xraypb.Envelope_StreamUpsert{
			StreamUpsert: streamUpsert,
		},
	})
	return ws
}

func directionFromNetwork(d network.Direction) Direction {
	switch d {
	case network.DirInbound:
		return DirectionIn
	case network.DirOutbound:
		return DirectionOut
	default:
		return DirectionUnknown
	}
}

func directionToIngest(d Direction) xraypb.Direction {
	switch d {
	case DirectionIn:
		return xraypb.Direction_DIRECTION_IN
	case DirectionOut:
		return xraypb.Direction_DIRECTION_OUT
	default:
		return xraypb.Direction_DIRECTION_UNKNOWN
	}
}
