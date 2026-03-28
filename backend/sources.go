package backend

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
	if len(r.sources) == 0 {
		return "", errors.New("no sources available")
	}
	if len(r.sources) == 1 {
		for id := range r.sources {
			return id, nil
		}
	}
	return "", errors.New("multiple sources available, specify ?source=<id>")
}
