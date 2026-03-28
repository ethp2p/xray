package introspector

import (
	"bytes"
	"testing"

	ingestpb "github.com/ethp2p/instrument/pb/ingest"
)

func TestCodecRoundTripClientHello(t *testing.T) {
	var buf bytes.Buffer
	hello := &ingestpb.ClientHello{
		ProtocolVersion: 2,
		PeerId:          []byte("test-peer"),
		ClientName:      "prysm/v5.2.0",
	}
	if err := WriteClientHello(&buf, hello); err != nil {
		t.Fatal(err)
	}
	got, err := ReadClientHello(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClientName != hello.ClientName {
		t.Fatalf("got %q, want %q", got.ClientName, hello.ClientName)
	}
}

func TestCodecRoundTripServerHello(t *testing.T) {
	var buf bytes.Buffer
	hello := &ingestpb.ServerHello{
		ProtocolVersion: 2,
		SourceId:        "16Uiu2HAm...",
	}
	if err := WriteServerHello(&buf, hello); err != nil {
		t.Fatal(err)
	}
	got, err := ReadServerHello(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceId != hello.SourceId {
		t.Fatalf("got %q, want %q", got.SourceId, hello.SourceId)
	}
}

func TestCodecRoundTripEnvelope(t *testing.T) {
	var buf bytes.Buffer
	env := &ingestpb.Envelope{
		Seq: 42,
		Payload: &ingestpb.Envelope_StringDef{
			StringDef: &ingestpb.StringDef{Id: 1, Value: "test"},
		},
	}
	if err := WriteEnvelope(&buf, env); err != nil {
		t.Fatal(err)
	}
	got, err := ReadEnvelope(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seq != 42 {
		t.Fatalf("got seq %d, want 42", got.Seq)
	}
}

func TestCodecRejectsUnknownDiscriminator(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0xFF, 0x00})
	_, err := ReadEnvelope(buf)
	if err == nil {
		t.Fatal("expected error for unknown discriminator")
	}
}
