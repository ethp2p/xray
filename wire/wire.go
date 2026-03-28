package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	ingestpb "github.com/ethp2p/instrument/pb/ingest"
	"google.golang.org/protobuf/proto"
)

const (
	WireClientHello byte = 0x01
	WireServerHello byte = 0x02
	WireEnvelope    byte = 0x03

	MaxMessageSize         = 4 << 20 // 4 MiB
	IngestProtocolVersion  uint32 = 2
)

func WriteClientHello(w io.Writer, msg *ingestpb.ClientHello) error {
	return WriteTyped(w, WireClientHello, msg)
}

func ReadClientHello(r io.Reader) (*ingestpb.ClientHello, error) {
	msg := &ingestpb.ClientHello{}
	if err := ReadTyped(r, WireClientHello, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func WriteServerHello(w io.Writer, msg *ingestpb.ServerHello) error {
	return WriteTyped(w, WireServerHello, msg)
}

func ReadServerHello(r io.Reader) (*ingestpb.ServerHello, error) {
	msg := &ingestpb.ServerHello{}
	if err := ReadTyped(r, WireServerHello, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func WriteEnvelope(w io.Writer, msg *ingestpb.Envelope) error {
	return WriteTyped(w, WireEnvelope, msg)
}

func ReadEnvelope(r io.Reader) (*ingestpb.Envelope, error) {
	msg := &ingestpb.Envelope{}
	if err := ReadTyped(r, WireEnvelope, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func WriteTyped(w io.Writer, typ byte, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := w.Write([]byte{typ}); err != nil {
		return err
	}
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(data)))
	if _, err := w.Write(buf[:n]); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func ReadTyped(r io.Reader, expectedType byte, msg proto.Message) error {
	var typBuf [1]byte
	if _, err := io.ReadFull(r, typBuf[:]); err != nil {
		return err
	}
	if typBuf[0] != expectedType {
		return fmt.Errorf("unexpected message type: got 0x%02x, want 0x%02x", typBuf[0], expectedType)
	}
	return readDelimitedInto(r, msg)
}

func readDelimitedInto(r io.Reader, msg proto.Message) error {
	var length uint64
	var shift uint
	for {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return err
		}
		length |= uint64(b[0]&0x7f) << shift
		if b[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return errors.New("varint overflow")
		}
	}
	if length > MaxMessageSize {
		return fmt.Errorf("message too large: %d bytes", length)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return proto.Unmarshal(data, msg)
}

// InferNetwork returns "unix" if addr contains "/" (a socket path), else "tcp".
func InferNetwork(addr string) string {
	for i := range len(addr) {
		if addr[i] == '/' {
			return "unix"
		}
	}
	return "tcp"
}
