package introspector

import (
	"io"

	ingestpb "github.com/ethp2p/instrument/pb/ingest"
	"github.com/ethp2p/instrument/wire"
)

func WriteClientHello(w io.Writer, msg *ingestpb.ClientHello) error {
	return wire.WriteClientHello(w, msg)
}

func ReadClientHello(r io.Reader) (*ingestpb.ClientHello, error) {
	return wire.ReadClientHello(r)
}

func WriteServerHello(w io.Writer, msg *ingestpb.ServerHello) error {
	return wire.WriteServerHello(w, msg)
}

func ReadServerHello(r io.Reader) (*ingestpb.ServerHello, error) {
	return wire.ReadServerHello(r)
}

func WriteEnvelope(w io.Writer, msg *ingestpb.Envelope) error {
	return wire.WriteEnvelope(w, msg)
}

func ReadEnvelope(r io.Reader) (*ingestpb.Envelope, error) {
	return wire.ReadEnvelope(r)
}
