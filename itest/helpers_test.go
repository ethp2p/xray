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

	pb "github.com/ethp2p/xray/proto"
)

func readTraceFile(t *testing.T, path string) []*pb.TraceEvent {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var events []*pb.TraceEvent
	for {
		var e pb.TraceEvent
		if err := readDelimited(f, &e, 1<<20); err != nil {
			if err == io.EOF {
				break
			}
			break
		}
		events = append(events, &e)
	}
	return events
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

func assertHasEventType(t *testing.T, events []*pb.TraceEvent, eventType string) {
	t.Helper()
	assert.Greater(t, countEventType(events, eventType), 0,
		"should have %s events", eventType)
}

func countEventType(events []*pb.TraceEvent, eventType string) int {
	count := 0
	for _, e := range events {
		switch eventType {
		case "snapshot":
			if e.GetSnapshot() != nil {
				count++
			}
		case "string_def":
			if e.GetStringDef() != nil {
				count++
			}
		case "conn_opened":
			if e.GetConnOpened() != nil {
				count++
			}
		case "conn_closed":
			if e.GetConnClosed() != nil {
				count++
			}
		case "stream_opened":
			if e.GetStreamOpened() != nil {
				count++
			}
		case "stream_closed":
			if e.GetStreamClosed() != nil {
				count++
			}
		case "traffic":
			if e.GetTraffic() != nil {
				count++
			}
		}
	}
	return count
}

func sumTraffic(events []*pb.TraceEvent) (totalIn, totalOut uint64) {
	for _, e := range events {
		if bw := e.GetTraffic(); bw != nil {
			if bw.Direction == pb.Direction_DIRECTION_IN {
				totalIn += uint64(bw.Bytes)
			} else {
				totalOut += uint64(bw.Bytes)
			}
		}
	}
	return
}

func assertMonotonicSeq(t *testing.T, events []*pb.TraceEvent) {
	t.Helper()
	var lastNonZeroSeq uint64
	for _, e := range events {
		if e.Seq > 0 {
			assert.GreaterOrEqual(t, e.Seq, lastNonZeroSeq, "sequence should be monotonic: got %d after %d", e.Seq, lastNonZeroSeq)
			lastNonZeroSeq = e.Seq
		}
	}
}

func assertTimestampsInRange(t *testing.T, events []*pb.TraceEvent, start, end time.Time) {
	t.Helper()
	for _, e := range events {
		ts := time.Unix(0, e.TimestampNs)
		assert.True(t, ts.After(start) && ts.Before(end),
			"timestamp %v should be between %v and %v", ts, start, end)
	}
}

func extractStreamIDs(events []*pb.TraceEvent) []uint32 {
	var ids []uint32
	for _, e := range events {
		if so := e.GetStreamOpened(); so != nil {
			ids = append(ids, so.Info.StreamId)
		}
	}
	return ids
}

func unique(ids []uint32) []uint32 {
	seen := make(map[uint32]bool)
	var result []uint32
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
}

func allEqual(ids []uint32) bool {
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
