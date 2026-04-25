package xray

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	wiretappb "github.com/ethp2p/xray/proto/wiretap"
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
		if s.decoder != nil {
			s.worker.send(s, DirectionIn, p[:n])
		}
	}
	return n, err
}

func (s *wrappedStream) Write(p []byte) (int, error) {
	n, err := s.Stream.Write(p)
	if n > 0 {
		s.emitChunk(DirectionOut, p[:n])
		if s.decoder != nil {
			s.worker.send(s, DirectionOut, p[:n])
		}
	}
	return n, err
}

func (s *wrappedStream) emitChunk(dir Direction, data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)
	s.emitter.Emit(&wiretappb.Envelope{
		Payload: &wiretappb.Envelope_StreamChunk{
			StreamChunk: &wiretappb.StreamChunk{
				StreamAlias: uint64(s.streamID),
				Direction:   directionToIngest(dir),
				Data:        cp,
			},
		},
	})
}

func (s *wrappedStream) Close() error {
	s.emitClosed(wiretappb.CloseReason_CLOSE_REASON_CLOSE)
	return s.Stream.Close()
}

func (s *wrappedStream) Reset() error {
	s.emitClosed(wiretappb.CloseReason_CLOSE_REASON_RESET)
	return s.Stream.Reset()
}

func (s *wrappedStream) emitClosed(reason wiretappb.CloseReason) {
	s.net.removeStream(s.streamID)
	s.emitter.Emit(&wiretappb.Envelope{
		Payload: &wiretappb.Envelope_StreamClosed{
			StreamClosed: &wiretappb.StreamClosed{
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
	connectionUpserts map[uint32]*wiretappb.ConnectionUpsert
	streamUpserts     map[uint32]*wiretappb.StreamUpsert
}

// connectionUpsertFor builds a ConnectionUpsert from a libp2p connection,
// allocating a peer alias for the remote peer if it is new. Returns the
// wrappedConn (created or pre-existing), the upsert, and whether the
// connection was newly tracked.
func (n *wrappedNetwork) connectionUpsertFor(conn network.Conn, connID uint32, openedAtNs int64) (*wrappedConn, *wiretappb.ConnectionUpsert, bool) {
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

	state := conn.ConnState()
	upsert := &wiretappb.ConnectionUpsert{
		ConnAlias:   uint64(connID),
		PeerAlias:   peerInfo.alias,
		RemoteAddr:  conn.RemoteMultiaddr().String(),
		LocalAddr:   conn.LocalMultiaddr().String(),
		Direction:   directionToIngest(directionFromNetwork(conn.Stat().Direction)),
		TransportId: n.strings.Intern(state.Transport),
		SecurityId:  n.strings.Intern(string(state.Security)),
		MuxerId:     n.strings.Intern(string(state.StreamMultiplexer)),
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

func (n *wrappedNetwork) peerUpsertFor(alias uint64, id peer.ID) *wiretappb.PeerUpsert {
	return &wiretappb.PeerUpsert{
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

func (n *wrappedNetwork) addStreamUpsert(u *wiretappb.StreamUpsert) {
	n.mu.Lock()
	n.streamUpserts[uint32(u.StreamAlias)] = u
	n.mu.Unlock()
}

func (n *wrappedNetwork) removeStream(streamID uint32) {
	n.mu.Lock()
	delete(n.streamUpserts, streamID)
	n.mu.Unlock()
}

// snapshot returns the current state as ingest envelope payloads, suitable for
// replaying to a new sink to bring it up to date.
func (n *wrappedNetwork) snapshot() ([]*wiretappb.PeerUpsert, []*wiretappb.ConnectionUpsert, []*wiretappb.StreamUpsert) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	peers := make([]*wiretappb.PeerUpsert, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, n.peerUpsertFor(p.alias, p.peerID))
	}
	connections := make([]*wiretappb.ConnectionUpsert, 0, len(n.connectionUpserts))
	for _, c := range n.connectionUpserts {
		connections = append(connections, c)
	}
	streams := make([]*wiretappb.StreamUpsert, 0, len(n.streamUpserts))
	for _, s := range n.streamUpserts {
		streams = append(streams, s)
	}
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
		var upsert *wiretappb.ConnectionUpsert
		var created bool
		wc, upsert, created = n.connectionUpsertFor(s.Conn(), connID, openedAt)
		if created {
			n.emitter.Emit(&wiretappb.Envelope{
				Payload: &wiretappb.Envelope_PeerUpsert{
					PeerUpsert: n.peerUpsertFor(wc.peerAlias, s.Conn().RemotePeer()),
				},
			})
			n.emitter.Emit(&wiretappb.Envelope{
				Payload: &wiretappb.Envelope_ConnectionUpsert{
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

	streamUpsert := &wiretappb.StreamUpsert{
		StreamAlias: uint64(streamID),
		ConnAlias:   uint64(wc.connID),
		Direction:   directionToIngest(directionFromNetwork(s.Stat().Direction)),
		ProtocolId:  n.strings.Intern(proto),
		OpenedAtNs:  time.Now().UnixNano(),
	}
	n.addStreamUpsert(streamUpsert)
	n.emitter.Emit(&wiretappb.Envelope{
		Payload: &wiretappb.Envelope_StreamUpsert{
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

func directionToIngest(d Direction) wiretappb.Direction {
	switch d {
	case DirectionIn:
		return wiretappb.Direction_DIRECTION_IN
	case DirectionOut:
		return wiretappb.Direction_DIRECTION_OUT
	default:
		return wiretappb.Direction_DIRECTION_UNKNOWN
	}
}
