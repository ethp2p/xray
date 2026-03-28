package serve

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	instrument "github.com/ethp2p/instrument"
	pb "github.com/ethp2p/instrument/pb"
	pbconnect "github.com/ethp2p/instrument/pb/instrumentpbconnect"
)

const (
	clientSendBufferSize = 1000
	slowWarningInterval  = 100
)

// GrpcSink implements instrument.Sink and fans out events to connected gRPC clients.
type GrpcSink struct {
	mu      sync.Mutex
	clients map[*clientState]struct{}
}

type clientState struct {
	emitter *instrument.Emitter

	sendCh  chan *pb.ServerMessage
	dropped atomic.Uint64
	closed  atomic.Bool
}

// NewGrpcSink creates a new GrpcSink.
func NewGrpcSink() *GrpcSink {
	return &GrpcSink{
		clients: make(map[*clientState]struct{}),
	}
}

// Write sends an event to all connected clients.
func (s *GrpcSink) Write(event *pb.TraceEvent) bool {
	s.mu.Lock()
	clients := make([]*clientState, 0, len(s.clients))
	for c := range s.clients {
		clients = append(clients, c)
	}
	s.mu.Unlock()

	for _, c := range clients {
		c.offer(event)
	}
	return true
}

// Close shuts down all client connections.
func (s *GrpcSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for c := range s.clients {
		c.close()
	}
	return nil
}

func (s *GrpcSink) AddClient(cs *clientState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[cs] = struct{}{}
}

func (s *GrpcSink) RemoveClient(cs *clientState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, cs)
}

func newClientState(emitter *instrument.Emitter) *clientState {
	return &clientState{
		emitter: emitter,
		sendCh:  make(chan *pb.ServerMessage, clientSendBufferSize),
	}
}

func (c *clientState) offer(event *pb.TraceEvent) {
	if c.closed.Load() {
		return
	}

	select {
	case c.sendCh <- &pb.ServerMessage{Msg: &pb.ServerMessage_Event{Event: event}}:
	default:
		n := c.dropped.Add(1)
		if n%slowWarningInterval == 0 {
			c.sendSlowWarning()
		}
	}
}

func (c *clientState) sendSlowWarning() {
	warning := &pb.ServerMessage{
		Msg: &pb.ServerMessage_Slow{
			Slow: &pb.SlowWarning{
				BufferedEvents: uint64(len(c.sendCh)),
				DroppedEvents:  c.dropped.Load(),
			},
		},
	}

	select {
	case c.sendCh <- warning:
	default:
	}
}

func (c *clientState) close() {
	if c.closed.Swap(true) {
		return
	}
	close(c.sendCh)
}

// WiretapServer implements the WiretapService Connect handler.
type WiretapServer struct {
	pbconnect.UnimplementedWiretapServiceHandler
	emitter *instrument.Emitter
	sink    *GrpcSink
}

// NewWiretapServer creates a new Connect handler for the wiretap service.
func NewWiretapServer(emitter *instrument.Emitter, sink *GrpcSink) pbconnect.WiretapServiceHandler {
	return &WiretapServer{
		emitter: emitter,
		sink:    sink,
	}
}

// StreamEvents handles server-streaming for trace events.
func (s *WiretapServer) StreamEvents(
	ctx context.Context,
	req *connect.Request[pb.Handshake],
	stream *connect.ServerStream[pb.ServerMessage],
) error {
	cs := newClientState(s.emitter)

	if err := s.handleHandshake(stream, req.Msg); err != nil {
		return err
	}

	s.sink.AddClient(cs)
	defer func() {
		s.sink.RemoveClient(cs)
		cs.close()
	}()

	for {
		select {
		case msg, ok := <-cs.sendCh:
			if !ok {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *WiretapServer) handleHandshake(stream *connect.ServerStream[pb.ServerMessage], handshake *pb.Handshake) error {
	if handshake.ResumeFromSeq > 0 {
		events := s.emitter.EventsFromSeq(handshake.ResumeFromSeq)
		if events != nil {
			for _, e := range events {
				if err := stream.Send(&pb.ServerMessage{Msg: &pb.ServerMessage_Event{Event: e}}); err != nil {
					return err
				}
			}
		} else {
			if err := s.sendSnapshot(stream); err != nil {
				return err
			}
		}
	} else {
		if err := s.sendSnapshot(stream); err != nil {
			return err
		}
	}
	return nil
}

func (s *WiretapServer) sendSnapshot(stream *connect.ServerStream[pb.ServerMessage]) error {
	snap := s.emitter.Snapshot()
	event := &pb.TraceEvent{
		TimestampNs: time.Now().UnixNano(),
		Event:       &pb.TraceEvent_Snapshot{Snapshot: snap},
	}
	return stream.Send(&pb.ServerMessage{Msg: &pb.ServerMessage_Event{Event: event}})
}
