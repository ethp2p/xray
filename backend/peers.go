package backend

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
			summary.Connections = append(summary.Connections, *conn)
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
