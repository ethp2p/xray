package itest

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	wiretappb "github.com/ethp2p/xray/proto/wiretap"
)

func readTraceFile(t *testing.T, path string) []*wiretappb.Envelope {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var envs []*wiretappb.Envelope
	for {
		var e wiretappb.Envelope
		if err := readDelimited(f, &e, 1<<20); err != nil {
			if err == io.EOF {
				break
			}
			break
		}
		envs = append(envs, &e)
	}
	return envs
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

func assertHasPayloadType(t *testing.T, envs []*wiretappb.Envelope, kind string) {
	t.Helper()
	assert.Greater(t, countPayloadType(envs, kind), 0,
		"should have %s envelopes", kind)
}

func countPayloadType(envs []*wiretappb.Envelope, kind string) int {
	count := 0
	for _, e := range envs {
		switch kind {
		case "snapshot_start":
			if e.GetSnapshotStart() != nil {
				count++
			}
		case "snapshot_end":
			if e.GetSnapshotEnd() != nil {
				count++
			}
		case "string_def":
			if e.GetStringDef() != nil {
				count++
			}
		case "peer_upsert":
			if e.GetPeerUpsert() != nil {
				count++
			}
		case "connection_upsert":
			if e.GetConnectionUpsert() != nil {
				count++
			}
		case "connection_closed":
			if e.GetConnectionClosed() != nil {
				count++
			}
		case "stream_upsert":
			if e.GetStreamUpsert() != nil {
				count++
			}
		case "stream_closed":
			if e.GetStreamClosed() != nil {
				count++
			}
		case "stream_chunk":
			if e.GetStreamChunk() != nil {
				count++
			}
		}
	}
	return count
}

func sumStreamChunks(envs []*wiretappb.Envelope) (totalIn, totalOut uint64) {
	for _, e := range envs {
		if c := e.GetStreamChunk(); c != nil {
			if c.Direction == wiretappb.Direction_DIRECTION_IN {
				totalIn += uint64(len(c.Data))
			} else {
				totalOut += uint64(len(c.Data))
			}
		}
	}
	return
}

func assertMonotonicSeq(t *testing.T, envs []*wiretappb.Envelope) {
	t.Helper()
	var lastSeq uint64
	for _, e := range envs {
		if e.Seq > 0 {
			assert.GreaterOrEqual(t, e.Seq, lastSeq, "sequence should be monotonic: got %d after %d", e.Seq, lastSeq)
			lastSeq = e.Seq
		}
	}
}

func assertTimestampsInRange(t *testing.T, envs []*wiretappb.Envelope, start, end time.Time) {
	t.Helper()
	for _, e := range envs {
		ts := time.Unix(0, e.ObservedAtNs)
		assert.True(t, ts.After(start) && ts.Before(end),
			"timestamp %v should be between %v and %v", ts, start, end)
	}
}

func extractStreamAliases(envs []*wiretappb.Envelope) []uint64 {
	var ids []uint64
	for _, e := range envs {
		if so := e.GetStreamUpsert(); so != nil {
			ids = append(ids, so.StreamAlias)
		}
	}
	return ids
}

func unique[T comparable](ids []T) []T {
	seen := make(map[T]bool)
	var result []T
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
}

func allEqual[T comparable](ids []T) bool {
	if len(ids) == 0 {
		return true
	}
	first := ids[0]
	for _, id := range ids[1:] {
		if id != first {
			return false
		}
	}
	return true
}

func echoHandler(s network.Stream) {
	io.Copy(s, s)
	s.Close()
}
