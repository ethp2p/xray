package wire

import (
	"bytes"
	"testing"

	wiretappb "github.com/ethp2p/xray/proto/wiretap"
)

func TestRoundTripClientHello(t *testing.T) {
	var buf bytes.Buffer
	hello := &wiretappb.ClientHello{
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

func TestRoundTripEnvelope(t *testing.T) {
	var buf bytes.Buffer
	env := &wiretappb.Envelope{
		Seq: 42,
		Payload: &wiretappb.Envelope_StringDef{
			StringDef: &wiretappb.StringDef{Id: 1, Value: "test"},
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

func TestRejectsUnknownDiscriminator(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0xFF, 0x00})
	_, err := ReadEnvelope(buf)
	if err == nil {
		t.Fatal("expected error for unknown discriminator")
	}
}

func TestInferNetwork(t *testing.T) {
	if got := InferNetwork("/tmp/wiretap.sock"); got != "unix" {
		t.Fatalf("expected unix, got %s", got)
	}
	if got := InferNetwork("127.0.0.1:9100"); got != "tcp" {
		t.Fatalf("expected tcp, got %s", got)
	}
}
