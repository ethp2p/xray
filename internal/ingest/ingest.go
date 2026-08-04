package ingest

import (
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ethp2p/xray/internal/processor"
	"github.com/ethp2p/xray/internal/sources"
	"github.com/ethp2p/xray/internal/storage"
	xraypb "github.com/ethp2p/xray/proto/xray"
	"github.com/ethp2p/xray/proto/xray/wire"
)

// Listener accepts inbound connections from probes, performs a
// ClientHello/ServerHello handshake, then streams Envelope events into the
// Processor. Each unique source_id gets at most one active session; a
// reconnecting probe preempts the previous connection.
type Listener struct {
	ctx       context.Context
	processor *processor.Processor
	registry  *sources.Registry
	storage   *storage.Storage
	mu        sync.Mutex
	sessions  map[string]*activeSession
}

type activeSession struct {
	cancel func()
	conn   net.Conn
}

// CloseSession closes the conn for any active session matching sourceID.
// Returns true if a session was closed. The handleConnection goroutine
// observes the closed conn, exits its read loop, and runs its deferred
// cleanup, leaving the per-source cursor and aggregated state intact so the
// next attach can resume via replay.
func (l *Listener) CloseSession(sourceID string) bool {
	l.mu.Lock()
	sess, ok := l.sessions[sourceID]
	l.mu.Unlock()
	if !ok {
		return false
	}
	_ = sess.conn.Close()
	return true
}

func NewListener(p *processor.Processor, registry *sources.Registry, store *storage.Storage) *Listener {
	return &Listener{
		processor: p,
		registry:  registry,
		storage:   store,
		sessions:  make(map[string]*activeSession),
	}
}

// ListenAndServe binds to address and accepts probe connections until ctx is
// cancelled. Address is interpreted as a Unix socket path if it contains '/',
// otherwise as a TCP address.
func (l *Listener) ListenAndServe(ctx context.Context, address string) error {
	l.ctx = ctx

	network := wire.InferNetwork(address)
	if network == "unix" {
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			log.Printf("warning: could not remove stale socket %s: %v", address, err)
		}
	}

	ln, err := net.Listen(network, address)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		go l.handleConnection(conn)
	}
}

func (l *Listener) handleConnection(conn net.Conn) {
	hello, err := wire.ReadClientHello(conn)
	if err != nil {
		conn.Close()
		return
	}

	if hello.ProtocolVersion != wire.IngestProtocolVersion {
		conn.Close()
		return
	}

	sourceID := sources.DeriveSourceID(hello.PeerId)

	l.mu.Lock()
	if old, ok := l.sessions[sourceID]; ok {
		old.cancel()
		old.conn.Close()
	}
	ctx, cancel := context.WithCancel(l.ctx)
	l.sessions[sourceID] = &activeSession{cancel: cancel, conn: conn}
	l.mu.Unlock()

	defer func() {
		// Only clear Connected if this session still owns the slot. A faster
		// reconnect may have already replaced us; calling SetConnected(false)
		// unconditionally would mark the live successor as disconnected.
		l.mu.Lock()
		stillOwns := false
		if sess, ok := l.sessions[sourceID]; ok && sess.conn == conn {
			delete(l.sessions, sourceID)
			stillOwns = true
		}
		l.mu.Unlock()

		if stillOwns {
			l.registry.SetConnected(sourceID, false)
		}
		conn.Close()
		cancel()
	}()

	// First attach or probe restart (boot ID changed) means the prior cursor
	// and per-source alias maps are stale; reset before reading the cursor
	// so the probe sees last_acked_seq=0 and falls back to a full snapshot.
	prior, hadPrior := l.registry.Get(sourceID)
	if !hadPrior || !bytes.Equal(prior.BootID, hello.BootId) {
		l.processor.ResetSource(sourceID)
	}
	lastAcked := l.processor.LastAppliedSeq(sourceID)

	err = wire.WriteServerHello(conn, &xraypb.ServerHello{
		ProtocolVersion: wire.IngestProtocolVersion,
		SourceId:        sourceID,
		LastAckedSeq:    lastAcked,
	})
	if err != nil {
		return
	}

	info := sources.SourceInfo{
		SourceID:    sourceID,
		PeerID:      hello.PeerId,
		ClientName:  hello.ClientName,
		BootID:      hello.BootId,
		StartedAtNs: hello.StartedAtNs,
		ConnectedAt: time.Now(),
		Connected:   true,
	}
	l.registry.Register(info)
	if l.storage != nil {
		if err := l.storage.WriteSourceMeta(info); err != nil {
			log.Printf("ingest: failed to persist source meta for %s: %v", sourceID, err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		env, err := wire.ReadEnvelope(conn)
		if err != nil {
			return
		}
		l.processor.ApplyForSource(sourceID, env)
	}
}
