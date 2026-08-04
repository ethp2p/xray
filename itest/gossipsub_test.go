package itest

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio/protoio"

	"github.com/ethp2p/xray"
	"github.com/ethp2p/xray/internal/gossipsub"
	xraypb "github.com/ethp2p/xray/proto/xray"
	pspb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func TestGossipSub_TopicTagging(t *testing.T) {
	ctx := t.Context()

	tagCollector := newTagCollector()
	host1, host2, sink1, sink2 := setupTwoHostsWithPubsub(t, ctx, tagCollector)
	defer host1.Close()
	defer host2.Close()

	ps1, err := pubsub.NewGossipSub(ctx, host1)
	if err != nil {
		t.Fatalf("failed to create pubsub for host1: %v", err)
	}
	ps2, err := pubsub.NewGossipSub(ctx, host2)
	if err != nil {
		t.Fatalf("failed to create pubsub for host2: %v", err)
	}

	topicA, err := ps1.Join("topic-a")
	if err != nil {
		t.Fatalf("failed to join topic-a on host1: %v", err)
	}
	topicB, err := ps1.Join("topic-b")
	if err != nil {
		t.Fatalf("failed to join topic-b on host1: %v", err)
	}

	subA, err := topicA.Subscribe()
	if err != nil {
		t.Fatalf("failed to subscribe to topic-a: %v", err)
	}
	subB, err := topicB.Subscribe()
	if err != nil {
		t.Fatalf("failed to subscribe to topic-b: %v", err)
	}
	_ = subA
	_ = subB

	topicA2, err := ps2.Join("topic-a")
	if err != nil {
		t.Fatalf("failed to join topic-a on host2: %v", err)
	}
	topicB2, err := ps2.Join("topic-b")
	if err != nil {
		t.Fatalf("failed to join topic-b on host2: %v", err)
	}
	subA2, err := topicA2.Subscribe()
	if err != nil {
		t.Fatalf("failed to subscribe to topic-a on host2: %v", err)
	}
	subB2, err := topicB2.Subscribe()
	if err != nil {
		t.Fatalf("failed to subscribe to topic-b on host2: %v", err)
	}

	connectHosts(t, host1, host2)

	time.Sleep(2 * time.Second)

	if err := topicA.Publish(ctx, []byte("hello topic-a")); err != nil {
		t.Fatalf("failed to publish to topic-a: %v", err)
	}
	if err := topicB.Publish(ctx, []byte("hello topic-b")); err != nil {
		t.Fatalf("failed to publish to topic-b: %v", err)
	}

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 5*time.Second)
	defer receiveCancel()

	msgA, err := subA2.Next(receiveCtx)
	if err != nil {
		t.Fatalf("failed to receive message on topic-a: %v", err)
	}
	if !bytes.Equal(msgA.Data, []byte("hello topic-a")) {
		t.Errorf("got wrong message on topic-a: %s", string(msgA.Data))
	}

	msgB, err := subB2.Next(receiveCtx)
	if err != nil {
		t.Fatalf("failed to receive message on topic-b: %v", err)
	}
	if !bytes.Equal(msgB.Data, []byte("hello topic-b")) {
		t.Errorf("got wrong message on topic-b: %s", string(msgB.Data))
	}

	time.Sleep(200 * time.Millisecond)

	// Traffic events still carry byte counts (tagless)
	var totalBytes uint64
	for _, sink := range []*testSink{sink1, sink2} {
		for _, evt := range sink.Events() {
			if c := evt.GetStreamChunk(); c != nil {
				totalBytes += uint64(len(c.Data))
			}
		}
	}
	if totalBytes == 0 {
		t.Error("no traffic events recorded")
	}

	// Topics arrive via the async decode pipeline to OnMessage handlers
	topics := tagCollector.Topics()
	if _, ok := topics["topic-a"]; !ok {
		t.Error("topic-a not found in decoded messages")
	}
	if _, ok := topics["topic-b"]; !ok {
		t.Error("topic-b not found in decoded messages")
	}
}

func TestGossipSub_PartialFrameHandling(t *testing.T) {
	ctx := t.Context()

	tagCollector := newTagCollector()
	host1, host2, sink1, sink2 := setupTwoHostsWithPubsub(t, ctx, tagCollector)
	defer host1.Close()
	defer host2.Close()

	ps1, err := pubsub.NewGossipSub(ctx, host1)
	if err != nil {
		t.Fatalf("failed to create pubsub for host1: %v", err)
	}
	ps2, err := pubsub.NewGossipSub(ctx, host2)
	if err != nil {
		t.Fatalf("failed to create pubsub for host2: %v", err)
	}

	topic1, err := ps1.Join("test-topic")
	if err != nil {
		t.Fatalf("failed to join topic on host1: %v", err)
	}
	topic2, err := ps2.Join("test-topic")
	if err != nil {
		t.Fatalf("failed to join topic on host2: %v", err)
	}

	sub1, err := topic1.Subscribe()
	if err != nil {
		t.Fatalf("failed to subscribe on host1: %v", err)
	}
	sub2, err := topic2.Subscribe()
	if err != nil {
		t.Fatalf("failed to subscribe on host2: %v", err)
	}
	_ = sub1

	connectHosts(t, host1, host2)
	time.Sleep(2 * time.Second)

	messageSizes := []int{10, 100, 1000, 10000}
	for _, size := range messageSizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		if err := topic1.Publish(ctx, data); err != nil {
			t.Fatalf("failed to publish message of size %d: %v", size, err)
		}

		receiveCtx, receiveCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := sub2.Next(receiveCtx)
		receiveCancel()
		if err != nil {
			t.Fatalf("failed to receive message of size %d: %v", size, err)
		}
		if !bytes.Equal(msg.Data, data) {
			t.Errorf("message of size %d corrupted", size)
		}
	}

	time.Sleep(100 * time.Millisecond)

	var totalBytes uint64
	for _, sink := range []*testSink{sink1, sink2} {
		events := sink.Events()
		for _, evt := range events {
			if c := evt.GetStreamChunk(); c != nil {
				totalBytes += uint64(len(c.Data))
			}
		}
	}

	if totalBytes == 0 {
		t.Error("no traffic events recorded")
	}

	expectedMinBytes := uint64(10 + 100 + 1000 + 10000)
	if totalBytes < expectedMinBytes {
		t.Errorf("total bytes %d is less than minimum expected %d", totalBytes, expectedMinBytes)
	}
}

func TestGossipSub_DecoderFallback(t *testing.T) {
	ctx := t.Context()

	baseHost1, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		t.Fatalf("failed to create base host1: %v", err)
	}
	defer baseHost1.Close()

	baseHost2, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		t.Fatalf("failed to create base host2: %v", err)
	}
	defer baseHost2.Close()

	sink1 := newTestSink()
	tagCollector := newTagCollector()
	host1, err := xray.Wrap(baseHost1,
		xray.WithDecoder(gossipsub.Decoder{}.Match, gossipsub.Decoder{}.New),
		xray.WithOnMessage(tagCollector.Factory()),
		xray.WithSink(sink1),
	)
	if err != nil {
		t.Fatalf("failed to wrap host1: %v", err)
	}
	defer host1.Close()

	sink2 := newTestSink()
	host2, err := xray.Wrap(baseHost2,
		xray.WithDecoder(gossipsub.Decoder{}.Match, gossipsub.Decoder{}.New),
		xray.WithOnMessage(tagCollector.Factory()),
		xray.WithSink(sink2),
	)
	if err != nil {
		t.Fatalf("failed to wrap host2: %v", err)
	}
	defer host2.Close()

	receivedMalformed := make(chan struct{})
	host2.SetStreamHandler("/meshsub/1.1.0", func(s network.Stream) {
		buf := make([]byte, 1024)
		n, _ := s.Read(buf)
		if n > 0 {
			close(receivedMalformed)
		}
		s.Close()
	})

	connectHosts(t, host1, host2)
	time.Sleep(500 * time.Millisecond)

	stream, err := host1.NewStream(ctx, host2.ID(), "/meshsub/1.1.0")
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	malformedData := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01}
	malformedData = append(malformedData, bytes.Repeat([]byte("garbage"), 100)...)

	n, err := stream.Write(malformedData)
	if err != nil {
		t.Logf("write error (expected in fallback scenario): %v", err)
	}
	stream.Close()

	select {
	case <-receivedMalformed:
	case <-time.After(2 * time.Second):
		t.Log("host2 did not receive malformed data (may have been rejected)")
	}

	time.Sleep(100 * time.Millisecond)

	// Bytes are always counted on the hot path regardless of decoder errors
	var foundTrafficEvent bool
	events := sink1.Events()
	for _, evt := range events {
		if c := evt.GetStreamChunk(); c != nil && len(c.Data) > 0 {
			foundTrafficEvent = true
			if len(c.Data) >= n {
				t.Logf("found stream chunk with %d bytes (wrote %d)", len(c.Data), n)
			}
		}
	}

	if !foundTrafficEvent {
		t.Error("no traffic events recorded for malformed data")
	}
}

func TestGossipSub_ManualProtocolTraffic(t *testing.T) {
	ctx := t.Context()

	baseHost1, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		t.Fatalf("failed to create base host1: %v", err)
	}
	defer baseHost1.Close()

	baseHost2, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		t.Fatalf("failed to create base host2: %v", err)
	}
	defer baseHost2.Close()

	sink1 := newTestSink()
	tagCollector := newTagCollector()
	host1, err := xray.Wrap(baseHost1,
		xray.WithDecoder(gossipsub.Decoder{}.Match, gossipsub.Decoder{}.New),
		xray.WithOnMessage(tagCollector.Factory()),
		xray.WithSink(sink1),
	)
	if err != nil {
		t.Fatalf("failed to wrap host1: %v", err)
	}
	defer host1.Close()

	sink2 := newTestSink()
	host2, err := xray.Wrap(baseHost2,
		xray.WithDecoder(gossipsub.Decoder{}.Match, gossipsub.Decoder{}.New),
		xray.WithOnMessage(tagCollector.Factory()),
		xray.WithSink(sink2),
	)
	if err != nil {
		t.Fatalf("failed to wrap host2: %v", err)
	}
	defer host2.Close()

	receivedRPC := make(chan *pspb.RPC, 1)
	host2.SetStreamHandler("/meshsub/1.1.0", func(s network.Stream) {
		reader := protoio.NewDelimitedReader(s, 1<<20)
		var rpc pspb.RPC
		if err := reader.ReadMsg(&rpc); err == nil {
			receivedRPC <- &rpc
		}
		s.Close()
	})

	connectHosts(t, host1, host2)
	time.Sleep(500 * time.Millisecond)

	stream, err := host1.NewStream(ctx, host2.ID(), "/meshsub/1.1.0")
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	topicA := "manual-topic-a"
	topicB := "manual-topic-b"
	subscribe := true
	rpc := &pspb.RPC{
		Subscriptions: []*pspb.RPC_SubOpts{
			{Subscribe: &subscribe, Topicid: &topicA},
			{Subscribe: &subscribe, Topicid: &topicB},
		},
	}

	writer := protoio.NewDelimitedWriter(stream)
	if err := writer.WriteMsg(rpc); err != nil {
		t.Fatalf("failed to write RPC: %v", err)
	}
	stream.Close()

	select {
	case received := <-receivedRPC:
		if len(received.Subscriptions) != 2 {
			t.Errorf("expected 2 subscriptions, got %d", len(received.Subscriptions))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive RPC")
	}

	time.Sleep(200 * time.Millisecond)

	// Topics arrive via the async decode pipeline to OnMessage handlers
	topics := tagCollector.Topics()
	if _, ok := topics["manual-topic-a"]; !ok {
		t.Error("manual-topic-a not found in decoded messages")
	}
	if _, ok := topics["manual-topic-b"]; !ok {
		t.Error("manual-topic-b not found in decoded messages")
	}
}

// tagCollector records topics seen in decoded messages via OnMessage pipeline.
type tagCollector struct {
	mu     sync.Mutex
	topics map[string]struct{}
}

func newTagCollector() *tagCollector {
	return &tagCollector{topics: make(map[string]struct{})}
}

func (tc *tagCollector) Factory() xray.OnMessageFactory {
	return func(streamID uint32, protocol string) xray.OnMessage {
		return func(msg xray.DecodedMessage) {
			tc.mu.Lock()
			defer tc.mu.Unlock()
			for _, tag := range msg.Tags {
				if tag.Name == "topic" {
					for _, v := range tag.Values {
						tc.topics[v] = struct{}{}
					}
				}
			}
		}
	}
}

func (tc *tagCollector) Topics() map[string]struct{} {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	result := make(map[string]struct{}, len(tc.topics))
	for k, v := range tc.topics {
		result[k] = v
	}
	return result
}

type testSink struct {
	mu      sync.Mutex
	events  []*xraypb.Envelope
	strings map[uint32]string
}

func newTestSink() *testSink {
	return &testSink{
		strings: make(map[uint32]string),
	}
}

func (s *testSink) Write(event *xraypb.Envelope) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sd := event.GetStringDef(); sd != nil {
		s.strings[sd.Id] = sd.Value
	}

	s.events = append(s.events, event)
	return true
}

func (s *testSink) Close() error { return nil }

func (s *testSink) Events() []*xraypb.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*xraypb.Envelope, len(s.events))
	copy(result, s.events)
	return result
}

func (s *testSink) Strings() map[uint32]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[uint32]string, len(s.strings))
	for k, v := range s.strings {
		result[k] = v
	}
	return result
}

func setupTwoHostsWithPubsub(t *testing.T, ctx context.Context, tc *tagCollector) (*xray.Host, *xray.Host, *testSink, *testSink) {
	baseHost1, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		t.Fatalf("failed to create base host1: %v", err)
	}

	baseHost2, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	if err != nil {
		t.Fatalf("failed to create base host2: %v", err)
	}

	sink1 := newTestSink()
	host1, err := xray.Wrap(baseHost1,
		xray.WithDecoder(gossipsub.Decoder{}.Match, gossipsub.Decoder{}.New),
		xray.WithOnMessage(tc.Factory()),
		xray.WithSink(sink1),
	)
	if err != nil {
		baseHost1.Close()
		baseHost2.Close()
		t.Fatalf("failed to wrap host1: %v", err)
	}

	sink2 := newTestSink()
	host2, err := xray.Wrap(baseHost2,
		xray.WithDecoder(gossipsub.Decoder{}.Match, gossipsub.Decoder{}.New),
		xray.WithOnMessage(tc.Factory()),
		xray.WithSink(sink2),
	)
	if err != nil {
		host1.Close()
		baseHost2.Close()
		t.Fatalf("failed to wrap host2: %v", err)
	}

	t.Cleanup(func() {
		host1.Close()
		host2.Close()
	})

	return host1, host2, sink1, sink2
}

func connectHosts(t *testing.T, a, b host.Host) {
	t.Helper()
	err := b.Connect(context.Background(), peer.AddrInfo{ID: a.ID(), Addrs: a.Addrs()})
	if err != nil {
		t.Fatalf("failed to connect hosts: %v", err)
	}
}
