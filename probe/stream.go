package probe

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"

	pb "github.com/ethp2p/wiretap/proto"
	ingestpb "github.com/ethp2p/wiretap/proto/ingest"
)

// wrappedConn tracks the instrumentation connID on a connection.
type wrappedConn struct {
	network.Conn
	connID      uint32
	peerAlias   uint64
	openedAtNs  int64
	localAddr   string
	transportID uint32
	securityID  uint32
	muxerID     uint32
}

type trackedPeer struct {
	alias  uint64
	peerID peer.ID
}

// wrappedStream intercepts Read/Write to record bandwidth and forwards data
// to the async decode worker.
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
		if s.net.ingestSink != nil {
			s.net.ingestSink.EmitStreamChunk(uint64(s.streamID), DirectionIn, p[:n])
		}
		if s.decoder != nil {
			s.worker.send(s, DirectionIn, p[:n])
		} else {
			s.emitter.EmitTraffic(s.streamID, DirectionIn, n, nil)
		}
	}
	return n, err
}

func (s *wrappedStream) Write(p []byte) (int, error) {
	n, err := s.Stream.Write(p)
	if n > 0 {
		if s.net.ingestSink != nil {
			s.net.ingestSink.EmitStreamChunk(uint64(s.streamID), DirectionOut, p[:n])
		}
		if s.decoder != nil {
			s.worker.send(s, DirectionOut, p[:n])
		} else {
			s.emitter.EmitTraffic(s.streamID, DirectionOut, n, nil)
		}
	}
	return n, err
}

func (s *wrappedStream) Close() error {
	s.net.removeStream(s.streamID)
	s.emitter.Emit(&pb.TraceEvent{
		Event: &pb.TraceEvent_StreamClosed{
			StreamClosed: &pb.StreamClosed{StreamId: s.streamID},
		},
	})
	if s.net.ingestSink != nil {
		s.net.ingestSink.EmitStreamClosed(uint64(s.streamID), time.Now().UnixNano(), ingestpb.CloseReason_CLOSE_REASON_CLOSE)
	}
	return s.Stream.Close()
}

func (s *wrappedStream) Reset() error {
	s.net.removeStream(s.streamID)
	s.emitter.Emit(&pb.TraceEvent{
		Event: &pb.TraceEvent_StreamClosed{
			StreamClosed: &pb.StreamClosed{StreamId: s.streamID},
		},
	})
	if s.net.ingestSink != nil {
		s.net.ingestSink.EmitStreamClosed(uint64(s.streamID), time.Now().UnixNano(), ingestpb.CloseReason_CLOSE_REASON_RESET)
	}
	return s.Stream.Reset()
}

// wrappedNetwork wraps network.Network to intercept stream creation.
type wrappedNetwork struct {
	network.Network
	emitter    *Emitter
	strings    *StringInterner
	worker     *decodeWorker
	initDecode func(streamID uint32, protocol string) (StreamDecoder, []OnMessage)
	ingestSink *SinkIngest

	mu       sync.RWMutex
	peers    map[peer.ID]*trackedPeer
	conns    map[string]*wrappedConn
	connByID map[uint32]*wrappedConn
	connPBs  map[uint32]*pb.ConnInfo
	streams  map[uint32]*pb.StreamInfo
}

func (n *wrappedNetwork) connInfoFromConn(conn network.Conn, connID uint32, openedAtNs int64) (*pb.ConnInfo, uint32, uint32, uint32) {
	info := &pb.ConnInfo{
		ConnId:     connID,
		PeerId:     []byte(conn.RemotePeer()),
		RemoteAddr: conn.RemoteMultiaddr().String(),
		Direction:  directionToPb(directionFromNetwork(conn.Stat().Direction)),
		OpenedAtNs: openedAtNs,
	}

	state := conn.ConnState()
	info.Transport = state.Transport
	info.Security = string(state.Security)
	info.Muxer = string(state.StreamMultiplexer)

	transportID := n.strings.Intern(info.Transport)
	securityID := n.strings.Intern(info.Security)
	muxerID := n.strings.Intern(info.Muxer)
	return info, transportID, securityID, muxerID
}

func (n *wrappedNetwork) addConn(conn network.Conn, connID uint32, openedAtNs int64, info *pb.ConnInfo, localAddr string, transportID, securityID, muxerID uint32) (*wrappedConn, bool) {
	n.mu.Lock()
	if existing := n.conns[conn.ID()]; existing != nil {
		n.mu.Unlock()
		return existing, false
	}
	peerInfo := n.peers[conn.RemotePeer()]
	if peerInfo == nil {
		peerInfo = &trackedPeer{
			alias:  n.emitter.NextPeerAlias(),
			peerID: conn.RemotePeer(),
		}
		n.peers[conn.RemotePeer()] = peerInfo
	}
	wc := &wrappedConn{
		Conn:        conn,
		connID:      connID,
		peerAlias:   peerInfo.alias,
		openedAtNs:  openedAtNs,
		localAddr:   localAddr,
		transportID: transportID,
		securityID:  securityID,
		muxerID:     muxerID,
	}
	n.conns[conn.ID()] = wc
	n.connByID[connID] = wc
	n.connPBs[connID] = info
	n.mu.Unlock()
	return wc, true
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
		delete(n.connPBs, wc.connID)
	}
	n.mu.Unlock()
	return wc
}

func (n *wrappedNetwork) addStream(info *pb.StreamInfo) {
	n.mu.Lock()
	n.streams[info.StreamId] = info
	n.mu.Unlock()
}

func (n *wrappedNetwork) removeStream(streamID uint32) {
	n.mu.Lock()
	delete(n.streams, streamID)
	n.mu.Unlock()
}

func (n *wrappedNetwork) snapshot() ([]*pb.ConnInfo, []*pb.StreamInfo) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	conns := make([]*pb.ConnInfo, 0, len(n.connPBs))
	for _, c := range n.connPBs {
		conns = append(conns, c)
	}
	streams := make([]*pb.StreamInfo, 0, len(n.streams))
	for _, s := range n.streams {
		streams = append(streams, s)
	}
	return conns, streams
}

func (n *wrappedNetwork) ingestSnapshot() ([]*ingestpb.PeerUpsert, []*ingestpb.ConnectionUpsert, []*ingestpb.StreamUpsert) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	peers := make([]*ingestpb.PeerUpsert, 0, len(n.peers))
	for _, peerInfo := range n.peers {
		peers = append(peers, &ingestpb.PeerUpsert{
			PeerAlias: peerInfo.alias,
			PeerId:    []byte(peerInfo.peerID),
		})
	}

	connections := make([]*ingestpb.ConnectionUpsert, 0, len(n.connByID))
	for connID, wc := range n.connByID {
		info := n.connPBs[connID]
		if info == nil {
			continue
		}
		connections = append(connections, buildIngestConnectionUpsert(wc, info))
	}

	streams := make([]*ingestpb.StreamUpsert, 0, len(n.streams))
	for _, stream := range n.streams {
		streams = append(streams, buildIngestStreamUpsert(stream))
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
		info, transportID, securityID, muxerID := n.connInfoFromConn(s.Conn(), connID, openedAt)
		var created bool
		wc, created = n.addConn(s.Conn(), connID, openedAt, info, s.Conn().LocalMultiaddr().String(), transportID, securityID, muxerID)
		if created {
			n.emitter.Emit(&pb.TraceEvent{
				Event: &pb.TraceEvent_ConnOpened{
					ConnOpened: &pb.ConnOpened{Info: info},
				},
			})
			if n.ingestSink != nil {
				n.ingestSink.EmitPeerUpsert(wc.peerAlias, []byte(s.Conn().RemotePeer()))
				n.ingestSink.EmitConnectionUpsert(buildIngestConnectionUpsert(wc, info))
			}
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
	// Protocol is already negotiated for both inbound streams (delivered to
	// stream handlers) and outbound streams (returned from NewStream).
	// Initialise the decoder here rather than waiting for SetProtocol.
	if proto != "" && n.initDecode != nil {
		ws.decoder, ws.handlers = n.initDecode(streamID, proto)
	}
	info := &pb.StreamInfo{
		StreamId:   streamID,
		ConnId:     wc.connID,
		ProtocolId: n.strings.Intern(proto),
		Direction:  directionToPb(directionFromNetwork(s.Stat().Direction)),
		OpenedAtNs: time.Now().UnixNano(),
	}
	n.addStream(info)
	n.emitter.Emit(&pb.TraceEvent{
		Event: &pb.TraceEvent_StreamOpened{
			StreamOpened: &pb.StreamOpened{Info: info},
		},
	})
	if n.ingestSink != nil {
		n.ingestSink.EmitStreamUpsert(buildIngestStreamUpsert(info))
	}
	return ws
}

func buildIngestConnectionUpsert(wc *wrappedConn, info *pb.ConnInfo) *ingestpb.ConnectionUpsert {
	return &ingestpb.ConnectionUpsert{
		ConnAlias:   uint64(wc.connID),
		PeerAlias:   wc.peerAlias,
		RemoteAddr:  info.RemoteAddr,
		LocalAddr:   wc.localAddr,
		Direction:   directionToIngest(directionFromPb(info.Direction)),
		TransportId: wc.transportID,
		SecurityId:  wc.securityID,
		MuxerId:     wc.muxerID,
		OpenedAtNs:  info.OpenedAtNs,
	}
}

func buildIngestStreamUpsert(info *pb.StreamInfo) *ingestpb.StreamUpsert {
	return &ingestpb.StreamUpsert{
		StreamAlias: uint64(info.StreamId),
		ConnAlias:   uint64(info.ConnId),
		Direction:   directionToIngest(directionFromPb(info.Direction)),
		ProtocolId:  info.ProtocolId,
		OpenedAtNs:  info.OpenedAtNs,
	}
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

func directionToPb(d Direction) pb.Direction {
	switch d {
	case DirectionIn:
		return pb.Direction_DIRECTION_IN
	case DirectionOut:
		return pb.Direction_DIRECTION_OUT
	default:
		return pb.Direction_DIRECTION_UNKNOWN
	}
}

func directionFromPb(d pb.Direction) Direction {
	switch d {
	case pb.Direction_DIRECTION_IN:
		return DirectionIn
	case pb.Direction_DIRECTION_OUT:
		return DirectionOut
	default:
		return DirectionUnknown
	}
}
