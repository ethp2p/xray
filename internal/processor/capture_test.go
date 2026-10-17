package processor

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/ethp2p/xray/internal/eth"
	xraypb "github.com/ethp2p/xray/proto/xray"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func TestIncompleteSnapshotPreservesCursorAndCountsRawBytes(t *testing.T) {
	p := NewProcessor(eth.NewSlotClock(time.Unix(0, 0), 12))
	source := "probe"
	p.ApplyForSource(source, &xraypb.Envelope{Payload: &xraypb.Envelope_SnapshotStart{SnapshotStart: &xraypb.SnapshotStart{}}})
	p.ApplyForSource(source, &xraypb.Envelope{Payload: &xraypb.Envelope_StringDef{StringDef: &xraypb.StringDef{Id: 1, Value: "/meshsub/1.2.0"}}})
	p.ApplyForSource(source, &xraypb.Envelope{Payload: &xraypb.Envelope_StreamUpsert{StreamUpsert: &xraypb.StreamUpsert{StreamAlias: 1, ProtocolId: 1, CaptureStartedMidstream: true}}})
	p.ApplyForSource(source, &xraypb.Envelope{Payload: &xraypb.Envelope_SnapshotEnd{SnapshotEnd: &xraypb.SnapshotEnd{LastIncludedSeq: 100}}})
	chunk := &xraypb.Envelope{Seq: 101, ObservedAtNs: time.Unix(13, 0).UnixNano(), Payload: &xraypb.Envelope_StreamChunk{StreamChunk: &xraypb.StreamChunk{StreamAlias: 1, Direction: xraypb.Direction_DIRECTION_IN, Data: []byte{255, 255, 255}}}}
	p.ApplyForSource(source, chunk)
	p.ApplyForSource(source, chunk)
	chunk.Seq = 99
	p.ApplyForSource(source, chunk)
	detail, ok := p.SlotDetail(source, 1)
	if !ok || detail.Summary.BytesIn != 3 {
		t.Fatalf("unexpected slot accounting: %+v", detail)
	}
	if p.LastAppliedSeq(source) != 101 {
		t.Fatal("snapshot cursor was not preserved")
	}
	if len(detail.Breakdown) != 1 || detail.Breakdown[0].MessageKind != "capture_incomplete" {
		t.Fatalf("unexpected breakdown: %+v", detail.Breakdown)
	}
}

func TestGloasDigestAndEnvelopeTimestampAttribution(t *testing.T) {
	data, err := os.ReadFile("../eth/testdata/lodestar.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Genesis  int64 `json:"genesis"`
		Fixtures []struct {
			Name   string `json:"name"`
			Topic  string `json:"topic"`
			Snappy []byte `json:"snappy"`
		} `json:"fixtures"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, f := range fixture.Fixtures {
		if f.Name != "gloas_column" && f.Name != "envelope" {
			continue
		}
		t.Run(f.Name, func(t *testing.T) {
			p := NewProcessor(eth.NewSlotClock(time.Unix(fixture.Genesis, 0), 12), "11223344")
			p.ApplyForSource("probe", &xraypb.Envelope{Payload: &xraypb.Envelope_StringDef{StringDef: &xraypb.StringDef{Id: 1, Value: "/meshsub/1.2.0"}}})
			p.ApplyForSource("probe", &xraypb.Envelope{Payload: &xraypb.Envelope_StreamUpsert{StreamUpsert: &xraypb.StreamUpsert{StreamAlias: 1, ProtocolId: 1}}})
			topic := "/eth2/11223344/" + f.Topic + "/ssz_snappy"
			rpc, err := (&pubsubpb.RPC{Publish: []*pubsubpb.Message{{Topic: &topic, Data: f.Snappy}}}).Marshal()
			if err != nil {
				t.Fatal(err)
			}
			frame := append(binary.AppendUvarint(nil, uint64(len(rpc))), rpc...)
			p.ApplyForSource("probe", &xraypb.Envelope{ObservedAtNs: time.Unix(fixture.Genesis+43*12, 0).UnixNano(), Payload: &xraypb.Envelope_StreamChunk{StreamChunk: &xraypb.StreamChunk{StreamAlias: 1, Direction: xraypb.Direction_DIRECTION_IN, Data: frame}}})
			detail, ok := p.SlotDetail("probe", 43)
			if !ok || detail.Summary.BytesIn != uint64(len(frame)) {
				t.Fatalf("wrong total attribution: %+v", detail)
			}
			var publish *SlotBreakdown
			for i := range detail.Breakdown {
				if detail.Breakdown[i].MessageKind == "PUBLISH" {
					publish = &detail.Breakdown[i]
				}
			}
			if publish == nil || publish.BytesIn == 0 || publish.BleedBytesIn != publish.BytesIn || publish.BleedByDistance["1"].BytesIn != publish.BytesIn {
				t.Fatalf("expected slot-42 payload to be attributed one slot late: %+v", publish)
			}
		})
	}
}
