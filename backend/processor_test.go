package backend

import (
	"testing"
	"time"

	"github.com/ethp2p/xray/eth"
	ingestpb "github.com/ethp2p/xray/proto/ingest"
)

func TestProcessorTracksSlotTrafficAndBreakdown(t *testing.T) {
	clock := eth.NewSlotClock(time.Unix(1606824023, 0), 12)
	p := NewProcessor(clock)

	const src = "test-source"

	p.ApplyForSource(src, &ingestpb.Envelope{
		Payload: &ingestpb.Envelope_StringDef{
			StringDef: &ingestpb.StringDef{Id: 1, Value: "/meshsub/1.1.0"},
		},
	})
	p.ApplyForSource(src, &ingestpb.Envelope{
		Payload: &ingestpb.Envelope_StreamUpsert{
			StreamUpsert: &ingestpb.StreamUpsert{
				StreamAlias: 1,
				ProtocolId:  1,
			},
		},
	})

	now := clock.GenesisTime.Add(15 * time.Second)
	event := &ingestpb.Envelope{
		ObservedAtNs: now.UnixNano(),
		Payload: &ingestpb.Envelope_StreamChunk{
			StreamChunk: &ingestpb.StreamChunk{
				StreamAlias: 1,
				Direction:   ingestpb.Direction_DIRECTION_IN,
				Data: []byte{
					12, 10, 10, 8, 1, 18, 6, 47, 116, 101, 115, 116, 1, 2, 3, 4,
				},
			},
		},
	}
	p.ApplyForSource(src, event)

	ref, ok := clock.At(now)
	if !ok {
		t.Fatal("expected slot ref")
	}

	detail, exists := p.SlotDetail(src, ref.Slot)
	if !exists {
		t.Fatal("expected slot detail")
	}
	if detail.Summary.BytesIn == 0 {
		t.Fatal("expected inbound bytes to be tracked")
	}
	if len(detail.Breakdown) == 0 {
		t.Fatal("expected decoded breakdown rows")
	}
}
