package xray

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"

	wiretappb "github.com/ethp2p/xray/proto/wiretap"
)

// Host wraps a libp2p host to provide instrumentation.
// It embeds the underlying host and intercepts stream and connection operations
// to record traffic and events.
type Host struct {
	host.Host
	network *wrappedNetwork

	emitter      *Emitter
	decodeWorker *decodeWorker
	ingestSink   *SinkIngest
}

// Network returns the wrapped network.
func (h *Host) Network() network.Network {
	return h.network
}

// NewStream opens a new stream to the peer and wraps it for instrumentation.
func (h *Host) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
	s, err := h.Host.NewStream(ctx, p, pids...)
	if err != nil {
		return nil, err
	}
	return h.network.wrapStream(s), nil
}

// SetStreamHandler sets a stream handler that wraps incoming streams for instrumentation.
func (h *Host) SetStreamHandler(pid protocol.ID, handler network.StreamHandler) {
	wrappedHandler := func(s network.Stream) {
		ws := h.network.wrapStream(s)
		handler(ws)
	}
	h.Host.SetStreamHandler(pid, wrappedHandler)
}

// SetStreamHandlerMatch sets a stream handler with a matching function that wraps
// incoming streams for instrumentation.
func (h *Host) SetStreamHandlerMatch(pid protocol.ID, match func(protocol.ID) bool, handler network.StreamHandler) {
	wrappedHandler := func(s network.Stream) {
		ws := h.network.wrapStream(s)
		handler(ws)
	}
	h.Host.SetStreamHandlerMatch(pid, match, wrappedHandler)
}

// Emitter returns the event emitter for this host.
func (h *Host) Emitter() *Emitter {
	return h.emitter
}

// Close shuts down the instrumented host gracefully.
func (h *Host) Close() error {
	h.emitter.SetClosed(true)

	var wg sync.WaitGroup
	for _, sink := range h.emitter.Sinks() {
		wg.Go(func() { sink.Close() })
	}
	wg.Wait()

	if h.decodeWorker != nil {
		h.decodeWorker.stop()
	}

	if h.ingestSink != nil {
		h.ingestSink.Close()
	}

	return h.Host.Close()
}

// Wiretap creates an instrumented host that records traffic and connection events.
func Wiretap(h host.Host, opts ...Option) (*Host, error) {
	cfg := &config{
		ringBufferSize: DefaultRingBufferSize,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	emitter := NewEmitter(cfg.ringBufferSize)
	strings := newStringInterner(emitter)
	emitter.strings = strings

	var worker *decodeWorker
	var initDecode func(streamID uint32, proto string) (StreamDecoder, []OnMessage)
	if len(cfg.decoders) > 0 && len(cfg.onMessage) > 0 {
		worker = newDecodeWorker()
		factories := cfg.onMessage
		decoders := cfg.decoders
		initDecode = func(streamID uint32, proto string) (StreamDecoder, []OnMessage) {
			var inst StreamDecoder
			for _, entry := range decoders {
				if entry.match(proto) {
					inst = entry.decoderCtor()
					break
				}
			}
			if inst == nil {
				return nil, nil
			}
			var handlers []OnMessage
			for _, factory := range factories {
				if fn := factory(streamID, proto); fn != nil {
					handlers = append(handlers, fn)
				}
			}
			if len(handlers) == 0 {
				return nil, nil
			}
			return inst, handlers
		}
	}

	wrappedNet := &wrappedNetwork{
		Network:           h.Network(),
		emitter:           emitter,
		strings:           strings,
		worker:            worker,
		initDecode:        initDecode,
		peers:             make(map[peer.ID]*trackedPeer),
		conns:             make(map[string]*wrappedConn),
		connByID:          make(map[uint32]*wrappedConn),
		connectionUpserts: make(map[uint32]*wiretappb.ConnectionUpsert),
		streamUpserts:     make(map[uint32]*wiretappb.StreamUpsert),
	}
	emitter.net = wrappedNet

	instrumentedHost := &Host{
		Host:         h,
		emitter:      emitter,
		decodeWorker: worker,
		network:      wrappedNet,
	}

	for _, sink := range cfg.sinks {
		emitter.AddSink(sink)
	}

	if cfg.sinkFilePath != "" {
		fileSink, err := NewSinkFile(cfg.sinkFilePath, emitter, cfg.sinkFileOpts...)
		if err != nil {
			return nil, err
		}
		emitter.AddSink(fileSink)
	}

	if cfg.ingestAddr != "" {
		ingestSink := NewSinkIngest(cfg.ingestAddr, emitter, cfg.clientName, []byte(h.ID()), cfg.waitForAttach)
		instrumentedHost.ingestSink = ingestSink
		emitter.AddSink(ingestSink)
	}

	h.Network().Notify(&notifiee{
		emitter: emitter,
		net:     wrappedNet,
	})

	if instrumentedHost.ingestSink != nil && cfg.waitForAttach {
		if err := instrumentedHost.ingestSink.WaitForAttach(); err != nil {
			instrumentedHost.ingestSink.Close()
			return nil, err
		}
	}

	return instrumentedHost, nil
}

// notifiee implements network.Notifiee to track connection events.
type notifiee struct {
	emitter *Emitter
	net     *wrappedNetwork
}

var _ network.Notifiee = (*notifiee)(nil)

func (n *notifiee) Listen(network.Network, ma.Multiaddr)      {}
func (n *notifiee) ListenClose(network.Network, ma.Multiaddr) {}

func (n *notifiee) Connected(_ network.Network, conn network.Conn) {
	openedAt := time.Now().UnixNano()
	connID := n.emitter.NextConnID()
	wc, upsert, created := n.net.connectionUpsertFor(conn, connID, openedAt)
	if !created {
		return
	}
	n.emitter.Emit(&wiretappb.Envelope{
		Payload: &wiretappb.Envelope_PeerUpsert{
			PeerUpsert: n.net.peerUpsertFor(wc.peerAlias, conn.RemotePeer()),
		},
	})
	n.emitter.Emit(&wiretappb.Envelope{
		Payload: &wiretappb.Envelope_ConnectionUpsert{
			ConnectionUpsert: upsert,
		},
	})
}

func (n *notifiee) Disconnected(_ network.Network, conn network.Conn) {
	wc := n.net.removeAndGetConn(conn)
	if wc == nil {
		return
	}
	n.emitter.Emit(&wiretappb.Envelope{
		Payload: &wiretappb.Envelope_ConnectionClosed{
			ConnectionClosed: &wiretappb.ConnectionClosed{
				ConnAlias:  uint64(wc.connID),
				ClosedAtNs: time.Now().UnixNano(),
			},
		},
	})
}
