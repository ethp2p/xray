package processor

import (
	"testing"
	"time"

	"github.com/ethp2p/xray/internal/eth"
	xraypb "github.com/ethp2p/xray/proto/xray"
)

func TestProcessorTracksSlotTrafficAndBreakdown(t *testing.T) {
	clock := eth.NewSlotClock(time.Unix(1606824023, 0), 12)
	p := NewProcessor(clock)

	const src = "test-source"

	p.ApplyForSource(src, &xraypb.Envelope{
		Payload: &xraypb.Envelope_StringDef{
			StringDef: &xraypb.StringDef{Id: 1, Value: "/meshsub/1.1.0"},
		},
	})
	p.ApplyForSource(src, &xraypb.Envelope{
		Payload: &xraypb.Envelope_StreamUpsert{
			StreamUpsert: &xraypb.StreamUpsert{
				StreamAlias: 1,
				ProtocolId:  1,
			},
		},
	})

	now := clock.GenesisTime.Add(15 * time.Second)
	event := &xraypb.Envelope{
		ObservedAtNs: now.UnixNano(),
		Payload: &xraypb.Envelope_StreamChunk{
			StreamChunk: &xraypb.StreamChunk{
				StreamAlias: 1,
				Direction:   xraypb.Direction_DIRECTION_IN,
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
