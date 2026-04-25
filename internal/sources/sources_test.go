package sources

import (
	"testing"
	"time"
)

func TestSourceIDDeterministic(t *testing.T) {
	peerID := []byte{0x00, 0x24, 0x08, 0x01, 0x12, 0x20, 0xAB}
	id1 := DeriveSourceID(peerID)
	id2 := DeriveSourceID(peerID)
	if id1 != id2 {
		t.Fatalf("source_id not deterministic: %q != %q", id1, id2)
	}
	if id1 == "" {
		t.Fatal("source_id should not be empty")
	}
}

func TestSourceRegistryRegisterAndList(t *testing.T) {
	reg := NewSourceRegistry()
	info := SourceInfo{
		SourceID:    "src-1",
		PeerID:      []byte("peer1"),
		ClientName:  "prysm/v5.2.0",
		Connected:   true,
		ConnectedAt: time.Now(),
	}
	reg.Register(info)

	sources := reg.List()
	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(sources))
	}
	if sources[0].ClientName != "prysm/v5.2.0" {
		t.Fatalf("wrong client name: %s", sources[0].ClientName)
	}
}

func TestSourceRegistryDefaultSource(t *testing.T) {
	reg := NewSourceRegistry()

	_, err := reg.DefaultSourceID()
	if err == nil {
		t.Fatal("expected error with no sources")
	}

	reg.Register(SourceInfo{SourceID: "src-1"})
	id, err := reg.DefaultSourceID()
	if err != nil {
		t.Fatal(err)
	}
	if id != "src-1" {
		t.Fatalf("expected src-1, got %s", id)
	}

	reg.Register(SourceInfo{SourceID: "src-2"})
	_, err = reg.DefaultSourceID()
	if err == nil {
		t.Fatal("expected error with multiple sources")
	}
}

func TestSourceRegistrySetConnected(t *testing.T) {
	reg := NewSourceRegistry()
	reg.Register(SourceInfo{SourceID: "src-1", Connected: true})
	reg.SetConnected("src-1", false)
	info, ok := reg.Get("src-1")
	if !ok {
		t.Fatal("source not found")
	}
	if info.Connected {
		t.Fatal("expected connected=false")
	}
}
