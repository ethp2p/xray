package processor

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

func TestPeerMapClear(t *testing.T) {
	pm := NewPeerMap()
	pm.UpsertPeer(1, []byte("peer-abc"))
	pm.UpsertConnection(1, ConnState{PeerAlias: 1, RemoteAddr: "addr1", OpenedAtNs: 1000})
	pm.Clear()
	if pm.Count() != 0 {
		t.Fatalf("expected 0 peers after clear, got %d", pm.Count())
	}
}
