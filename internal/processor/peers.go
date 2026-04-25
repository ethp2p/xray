package processor

import (
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ethp2p/xray/api"
)

// ConnState extends api.ConnState with the internal PeerAlias used to look up
// the owning peer. Embedding lets ListPeers project to the public type with a
// single field reference instead of a manual copy.
type ConnState struct {
	api.ConnState
	PeerAlias uint64 `json:"-"`
}

// PeerSummary aliases api.PeerSummary for the JSON response.
type PeerSummary = api.PeerSummary

type PeerState struct {
	PeerID      []byte
	Connections map[uint64]*ConnState
	FirstSeenNs int64
	LastSeenNs  int64
}

type PeerMap struct {
	peers map[uint64]*PeerState // peer_alias -> PeerState
	conns map[uint64]uint64     // conn_alias -> peer_alias
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
		if len(ps.Connections) == 0 {
			continue
		}
		summary := PeerSummary{
			PeerID:      peer.ID(ps.PeerID).String(),
			FirstSeenNs: ps.FirstSeenNs,
			LastSeenNs:  ps.LastSeenNs,
		}
		for _, conn := range ps.Connections {
			summary.Connections = append(summary.Connections, conn.ConnState)
		}
		result = append(result, summary)
	}
	return result
}

func (m *PeerMap) Count() int {
	n := 0
	for _, ps := range m.peers {
		if len(ps.Connections) > 0 {
			n++
		}
	}
	return n
}

func (m *PeerMap) Clear() {
	m.peers = make(map[uint64]*PeerState)
	m.conns = make(map[uint64]uint64)
}
