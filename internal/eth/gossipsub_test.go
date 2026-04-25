package eth

import (
	"encoding/binary"
	"testing"

	"github.com/ethp2p/xray"
	"github.com/gogo/protobuf/proto"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// emittedAction captures one emit call from the decoder.
type emittedAction struct {
	bytes int
	tags  map[string][]string
}

func collectActions(frame []byte) ([]emittedAction, error) {
	decoder := GossipSubDecoder().New()
	var actions []emittedAction
	err := decoder.ObserveRead(frame, func(bytes int, tags []xray.Tag, _ any) {
		m := make(map[string][]string, len(tags))
		for _, tag := range tags {
			m[tag.Name] = append(m[tag.Name], tag.Values...)
		}
		actions = append(actions, emittedAction{bytes: bytes, tags: m})
	})
	return actions, err
}

func makeFrame(rpc *pubsubpb.RPC) ([]byte, error) {
	payload, err := proto.Marshal(rpc)
	if err != nil {
		return nil, err
	}
	var header [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(header[:], uint64(len(payload)))
	return append(header[:n], payload...), nil
}

func TestGossipSubDecoder_SubscribeEmit(t *testing.T) {
	topic := "/eth2/11223344/beacon_block/ssz_snappy"
	subscribe := true
	frame, err := makeFrame(&pubsubpb.RPC{
		Subscriptions: []*pubsubpb.RPC_SubOpts{
			{Subscribe: &subscribe, Topicid: &topic},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	actions, err := collectActions(frame)
	if err != nil {
		t.Fatalf("observe: %v", err)
	}

	// Expect 1 subscription emit + 1 framing emit.
	var sub *emittedAction
	for i := range actions {
		if actions[i].tags["message_kind"] != nil && actions[i].tags["message_kind"][0] == "SUBSCRIBE" {
			sub = &actions[i]
		}
	}
	if sub == nil {
		t.Fatal("no SUBSCRIBE emit")
	}
	// Subscription emits raw topic — normalization only applies to published messages.
	if got := sub.tags["topic"]; len(got) != 1 || got[0] != topic {
		t.Errorf("topic = %v, want [%s]", got, topic)
	}
}

func TestGossipSubDecoder_PublishEmit(t *testing.T) {
	topic := "/eth2/11223344/beacon_block/ssz_snappy"
	// 8 bytes of data — too short to decode a slot (beacon_block slot offset = 100).
	frame, err := makeFrame(&pubsubpb.RPC{
		Publish: []*pubsubpb.Message{
			{Topic: &topic, Data: []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	actions, err := collectActions(frame)
	if err != nil {
		t.Fatalf("observe: %v", err)
	}

	var pub *emittedAction
	for i := range actions {
		if actions[i].tags["message_kind"] != nil && actions[i].tags["message_kind"][0] == "PUBLISH" {
			pub = &actions[i]
		}
	}
	if pub == nil {
		t.Fatal("no PUBLISH emit")
	}
	// Topic is normalized by the eth message decoder.
	if got := pub.tags["topic"]; len(got) != 1 || got[0] != "beacon_block" {
		t.Errorf("topic = %v, want [beacon_block]", got)
	}
	// Payload too short — no slot tag.
	if pub.tags[xray.TagDecodedSlot] != nil {
		t.Errorf("unexpected slot tag: %v", pub.tags[xray.TagDecodedSlot])
	}
}

func TestGossipSubDecoder_ControlEmits(t *testing.T) {
	frame, err := makeFrame(&pubsubpb.RPC{
		Control: &pubsubpb.ControlMessage{
			Iwant: []*pubsubpb.ControlIWant{{}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	actions, err := collectActions(frame)
	if err != nil {
		t.Fatalf("observe: %v", err)
	}

	var found bool
	for _, a := range actions {
		if kinds := a.tags["message_kind"]; len(kinds) > 0 && kinds[0] == "IWANT" {
			found = true
		}
	}
	if !found {
		t.Error("no IWANT emit")
	}
}

func TestGossipSubDecoder_FramingEmit(t *testing.T) {
	topic := "/eth2/11223344/beacon_block/ssz_snappy"
	subscribe := true
	frame, err := makeFrame(&pubsubpb.RPC{
		Subscriptions: []*pubsubpb.RPC_SubOpts{
			{Subscribe: &subscribe, Topicid: &topic},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	actions, err := collectActions(frame)
	if err != nil {
		t.Fatalf("observe: %v", err)
	}

	// Framing emit carries only TagFraming; there must be at least one.
	var found bool
	for _, a := range actions {
		if _, ok := a.tags[xray.TagFraming]; ok {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected _framing emit")
	}
}
