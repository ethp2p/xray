package introspector

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"time"

	ingestpb "github.com/ethp2p/instrument/pb/ingest"
	"google.golang.org/protobuf/proto"
)

type UnixIngestClient struct {
	socketPath string
	processor  *Processor
}

func NewUnixIngestClient(socketPath string, processor *Processor) *UnixIngestClient {
	return &UnixIngestClient{
		socketPath: socketPath,
		processor:  processor,
	}
}

func (c *UnixIngestClient) Run() error {
	return c.RunContext(context.Background())
}

func (c *UnixIngestClient) RunContext(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := c.runOnce()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, os.ErrNotExist) {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (c *UnixIngestClient) runOnce() error {
	conn, err := net.Dial("unix", c.socketPath)
	if err != nil {
		return err
	}
	defer conn.Close()

	for {
		var event ingestpb.Envelope
		if err := readDelimited(conn, &event, 4<<20); err != nil {
			return err
		}
		c.processor.Apply(&event)
	}
}

func readDelimited(r io.Reader, msg proto.Message, maxSize int) error {
	var length uint64
	var shift uint
	for {
		var b [1]byte
		if _, err := r.Read(b[:]); err != nil {
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

	if int(length) > maxSize {
		return errors.New("message too large")
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return proto.Unmarshal(data, msg)
}

func writeDelimited(w io.Writer, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
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
