package itest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/ethp2p/instrument"
	"github.com/ethp2p/instrument/eth"
	"github.com/ethp2p/instrument/introspector"
)

type liveMessage struct {
	Type        string                    `json:"type"`
	Slot        *introspector.SlotSummary `json:"slot"`
	CurrentSlot uint64                    `json:"current_slot"`
}

func TestIntrospectorUnixIngestHTTPAndWS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fixture := startIntrospectorFixture(t, ctx)

	h1, err := libp2p.New()
	require.NoError(t, err)
	defer h1.Close()

	ih, err := instrument.Wrap(
		h1,
		instrument.WithUnixSocket(fixture.socketPath),
		instrument.WithWaitForAttach(),
	)
	require.NoError(t, err)
	defer ih.Close()

	h2, err := libp2p.New()
	require.NoError(t, err)
	defer h2.Close()

	wsConn := dialIntrospectorWS(t, fixture.baseURL)
	defer wsConn.Close()

	initial := readLiveMessage(t, wsConn)
	require.Equal(t, "snapshot", initial.Type)

	ih.SetStreamHandler(protocol.ID("/echo/1.0.0"), echoHandler)
	h2.Peerstore().AddAddrs(ih.ID(), ih.Addrs(), time.Hour)
	require.NoError(t, h2.Connect(ctx, peer.AddrInfo{ID: ih.ID(), Addrs: ih.Addrs()}))

	s, err := h2.NewStream(ctx, ih.ID(), protocol.ID("/echo/1.0.0"))
	require.NoError(t, err)
	_, err = s.Write([]byte("hello introspector"))
	require.NoError(t, err)
	require.NoError(t, s.CloseWrite())
	_, _ = io.ReadAll(s)
	require.NoError(t, s.Close())

	update := waitForSlotUpdate(t, wsConn)
	require.NotNil(t, update.Slot)
	require.Greater(t, update.Slot.BytesIn+update.Slot.BytesOut, uint64(0))

	require.Eventually(t, func() bool {
		slots := fetchSlotSummaries(t, fixture.baseURL)
		return len(slots) > 0 && slots[0].BytesIn > 0
	}, 5*time.Second, 100*time.Millisecond)
}

func TestIntrospectorGossipSubPublishE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	fixture := startIntrospectorFixture(t, ctx)

	baseHost1, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	require.NoError(t, err)
	defer baseHost1.Close()

	host1, err := instrument.Wrap(
		baseHost1,
		instrument.WithUnixSocket(fixture.socketPath),
		instrument.WithWaitForAttach(),
	)
	require.NoError(t, err)
	defer host1.Close()

	host2, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	require.NoError(t, err)
	defer host2.Close()

	ps1, err := pubsub.NewGossipSub(ctx, host1)
	require.NoError(t, err)
	ps2, err := pubsub.NewGossipSub(ctx, host2)
	require.NoError(t, err)

	topicName := "/eth2/01020304/beacon_block/ssz_snappy"
	topic1, err := ps1.Join(topicName)
	require.NoError(t, err)
	topic2, err := ps2.Join(topicName)
	require.NoError(t, err)

	sub2, err := topic2.Subscribe()
	require.NoError(t, err)

	connectHosts(t, host1, host2)
	time.Sleep(2 * time.Second)

	payload := make([]byte, 200)
	binary.LittleEndian.PutUint64(payload[100:], 424242)
	compressed := snappy.Encode(nil, payload)

	require.NoError(t, topic1.Publish(ctx, compressed))

	receiveCtx, receiveCancel := context.WithTimeout(ctx, 10*time.Second)
	defer receiveCancel()
	msg, err := sub2.Next(receiveCtx)
	require.NoError(t, err)
	require.Equal(t, compressed, msg.Data)

	var targetSlot uint64
	require.Eventually(t, func() bool {
		slots := fetchSlotSummaries(t, fixture.baseURL)
		if len(slots) == 0 {
			return false
		}
		for _, row := range slots {
			detail, statusCode := fetchSlotDetail(
				t,
				fixture.baseURL,
				row.Slot,
				map[string]string{
					"topic":        "beacon_block",
					"message_kind": "PUBLISH",
				},
			)
			if statusCode != http.StatusOK {
				continue
			}
			for _, breakdown := range detail.Breakdown {
				if strings.HasPrefix(breakdown.Protocol, "/meshsub/") &&
					breakdown.Topic == "beacon_block" &&
					breakdown.MessageKind == "PUBLISH" &&
					breakdown.BytesOut > 0 {
					targetSlot = row.Slot
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond)

	detail, statusCode := fetchSlotDetail(
		t,
		fixture.baseURL,
		targetSlot,
		map[string]string{
			"topic":        "beacon_block",
			"message_kind": "PUBLISH",
		},
	)
	require.Equal(t, http.StatusOK, statusCode)
	require.NotEmpty(t, detail.Breakdown)

	found := false
	for _, row := range detail.Breakdown {
		if strings.HasPrefix(row.Protocol, "/meshsub/") &&
			row.Topic == "beacon_block" &&
			row.MessageKind == "PUBLISH" &&
			row.BytesOut > 0 {
			found = true
			break
		}
	}
	require.True(t, found, "expected decoded gossipsub publish breakdown row")
}

type introspectorFixture struct {
	socketPath string
	baseURL    string
}

func startIntrospectorFixture(t *testing.T, ctx context.Context) introspectorFixture {
	t.Helper()

	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("wiretap-introspector-%d.sock", time.Now().UnixNano()))
	t.Cleanup(func() {
		_ = os.Remove(socketPath)
	})

	clock := eth.NewSlotClock(time.Unix(1606824023, 0), 12)
	processor := introspector.NewProcessor(clock)
	ingestClient := introspector.NewUnixIngestClient(socketPath, processor)

	ingestCtx, ingestCancel := context.WithCancel(ctx)
	t.Cleanup(ingestCancel)
	go func() {
		_ = ingestClient.RunContext(ingestCtx)
	}()

	server := introspector.NewServer(processor)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = listener.Close()
	})
	go func() {
		_ = server.Serve(listener)
	}()

	return introspectorFixture{
		socketPath: socketPath,
		baseURL:    "http://" + listener.Addr().String(),
	}
}

func fetchSlotSummaries(t *testing.T, baseURL string) []introspector.SlotSummary {
	t.Helper()

	resp, err := http.Get(baseURL + "/api/slots")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload struct {
		Slots []introspector.SlotSummary `json:"slots"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	return payload.Slots
}

func fetchSlotDetail(t *testing.T, baseURL string, slot uint64, filters map[string]string) (introspector.SlotDetail, int) {
	t.Helper()

	u, err := url.Parse(fmt.Sprintf("%s/api/slots/%d", baseURL, slot))
	require.NoError(t, err)
	query := u.Query()
	for key, value := range filters {
		query.Set(key, value)
	}
	u.RawQuery = query.Encode()

	resp, err := http.Get(u.String())
	require.NoError(t, err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return introspector.SlotDetail{}, resp.StatusCode
	}

	var detail introspector.SlotDetail
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&detail))
	return detail, resp.StatusCode
}

func dialIntrospectorWS(t *testing.T, baseURL string) *websocket.Conn {
	t.Helper()

	u, err := url.Parse(baseURL)
	require.NoError(t, err)
	u.Scheme = "ws"
	u.Path = "/api/ws"

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(15*time.Second)))
	return conn
}

func readLiveMessage(t *testing.T, conn *websocket.Conn) liveMessage {
	t.Helper()

	var msg liveMessage
	require.NoError(t, conn.ReadJSON(&msg))
	return msg
}

func waitForSlotUpdate(t *testing.T, conn *websocket.Conn) liveMessage {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		msg := readLiveMessage(t, conn)
		if msg.Type == "slot_update" && msg.Slot != nil {
			return msg
		}
	}
	t.Fatal("timed out waiting for slot update")
	return liveMessage{}
}
