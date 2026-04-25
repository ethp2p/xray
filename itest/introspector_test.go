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
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/ethp2p/xray/probe"
	"github.com/ethp2p/xray/eth"
	"github.com/ethp2p/xray/backend"
)

type liveMessage struct {
	Type        string                    `json:"type"`
	Slot        *backend.SlotSummary `json:"slot"`
	Slots       []backend.SlotSummary `json:"slots"`
	CurrentSlot uint64                    `json:"current_slot"`
}

func TestIntrospectorUnixIngestHTTPAndWS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fixture := startIntrospectorFixture(t, ctx)

	h1, err := libp2p.New()
	require.NoError(t, err)
	defer h1.Close()

	ih, err := probe.Wrap(
		h1,
		probe.WithUnixSocket(fixture.socketPath),
		probe.WithWaitForAttach(),
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

func TestMultiSourceIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fixture := startIntrospectorFixture(t, ctx)

	base1, err := libp2p.New()
	require.NoError(t, err)
	defer base1.Close()
	ih1, err := probe.Wrap(base1,
		probe.WithUnixSocket(fixture.socketPath),
		probe.WithClientName("client-1"),
		probe.WithWaitForAttach(),
	)
	require.NoError(t, err)
	defer ih1.Close()

	base2, err := libp2p.New()
	require.NoError(t, err)
	defer base2.Close()
	ih2, err := probe.Wrap(base2,
		probe.WithUnixSocket(fixture.socketPath),
		probe.WithClientName("client-2"),
		probe.WithWaitForAttach(),
	)
	require.NoError(t, err)
	defer ih2.Close()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fixture.baseURL + "/api/sources")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var payload struct {
			Sources []struct {
				SourceID   string `json:"source_id"`
				ClientName string `json:"client_name"`
			} `json:"sources"`
		}
		json.NewDecoder(resp.Body).Decode(&payload)
		return len(payload.Sources) >= 2
	}, 5*time.Second, 200*time.Millisecond, "expected 2 sources")

	resp, err := http.Get(fixture.baseURL + "/api/sources")
	require.NoError(t, err)
	defer resp.Body.Close()
	var sourcesPayload struct {
		Sources []struct {
			ClientName string `json:"client_name"`
		} `json:"sources"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&sourcesPayload))
	names := make(map[string]bool)
	for _, s := range sourcesPayload.Sources {
		names[s.ClientName] = true
	}
	require.True(t, names["client-1"], "client-1 should be registered")
	require.True(t, names["client-2"], "client-2 should be registered")
}

func TestPeersEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fixture := startIntrospectorFixture(t, ctx)

	base1, err := libp2p.New()
	require.NoError(t, err)
	defer base1.Close()
	ih1, err := probe.Wrap(base1,
		probe.WithUnixSocket(fixture.socketPath),
		probe.WithClientName("test-probe"),
		probe.WithWaitForAttach(),
	)
	require.NoError(t, err)
	defer ih1.Close()

	peer1, err := libp2p.New()
	require.NoError(t, err)
	defer peer1.Close()

	ih1.SetStreamHandler(protocol.ID("/echo/1.0.0"), echoHandler)
	connectHosts(t, ih1, peer1)

	var sourceID string
	require.Eventually(t, func() bool {
		resp, err := http.Get(fixture.baseURL + "/api/sources")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var payload struct {
			Sources []struct {
				SourceID string `json:"source_id"`
			} `json:"sources"`
		}
		json.NewDecoder(resp.Body).Decode(&payload)
		if len(payload.Sources) > 0 {
			sourceID = payload.Sources[0].SourceID
			return true
		}
		return false
	}, 5*time.Second, 200*time.Millisecond)

	require.Eventually(t, func() bool {
		resp, err := http.Get(fixture.baseURL + "/api/peers?source=" + url.QueryEscape(sourceID))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var payload struct {
			Peers []struct {
				PeerID      string `json:"peer_id"`
				Connections []struct {
				} `json:"connections"`
			} `json:"peers"`
		}
		json.NewDecoder(resp.Body).Decode(&payload)
		return len(payload.Peers) > 0 && len(payload.Peers[0].Connections) > 0
	}, 5*time.Second, 200*time.Millisecond, "expected at least one peer with connections")
}

func TestWebSocketSourceScoping(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fixture := startIntrospectorFixture(t, ctx)

	base1, err := libp2p.New()
	require.NoError(t, err)
	defer base1.Close()
	ih1, err := probe.Wrap(base1,
		probe.WithUnixSocket(fixture.socketPath),
		probe.WithClientName("ws-test"),
		probe.WithWaitForAttach(),
	)
	require.NoError(t, err)
	defer ih1.Close()

	var sourceID string
	require.Eventually(t, func() bool {
		resp, err := http.Get(fixture.baseURL + "/api/sources")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var payload struct {
			Sources []struct {
				SourceID string `json:"source_id"`
			} `json:"sources"`
		}
		json.NewDecoder(resp.Body).Decode(&payload)
		if len(payload.Sources) > 0 {
			sourceID = payload.Sources[0].SourceID
			return true
		}
		return false
	}, 5*time.Second, 200*time.Millisecond)

	wsURL, err := url.Parse(fixture.baseURL)
	require.NoError(t, err)
	wsURL.Scheme = "ws"
	wsURL.Path = "/api/ws"
	wsURL.RawQuery = "source=" + url.QueryEscape(sourceID)
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL.String(), nil)
	require.NoError(t, err)
	defer wsConn.Close()
	wsConn.SetReadDeadline(time.Now().Add(15 * time.Second))

	var msg map[string]any
	require.NoError(t, wsConn.ReadJSON(&msg))
	require.Equal(t, "snapshot", msg["type"])

	peer1, err := libp2p.New()
	require.NoError(t, err)
	defer peer1.Close()
	ih1.SetStreamHandler(protocol.ID("/echo/1.0.0"), echoHandler)
	connectHosts(t, ih1, peer1)
	s, err := peer1.NewStream(ctx, ih1.ID(), protocol.ID("/echo/1.0.0"))
	require.NoError(t, err)
	s.Write([]byte("hello"))
	s.CloseWrite()
	io.ReadAll(s)
	s.Close()

	require.Eventually(t, func() bool {
		wsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		var update map[string]any
		if err := wsConn.ReadJSON(&update); err != nil {
			return false
		}
		return update["type"] == "slot_batch"
	}, 10*time.Second, 100*time.Millisecond, "expected to receive a slot_batch WS message")
}

func TestIntrospectorGossipSubPublishE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	fixture := startIntrospectorFixture(t, ctx)

	baseHost1, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
	require.NoError(t, err)
	defer baseHost1.Close()

	host1, err := probe.Wrap(
		baseHost1,
		probe.WithUnixSocket(fixture.socketPath),
		probe.WithWaitForAttach(),
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

func TestSessionExclusivityOnReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fixture := startIntrospectorFixture(t, ctx)

	// Generate a fixed key so both hosts share the same peer_id (and thus source_id).
	priv, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)

	base1, err := libp2p.New(libp2p.Identity(priv))
	require.NoError(t, err)

	ih1, err := probe.Wrap(base1,
		probe.WithUnixSocket(fixture.socketPath),
		probe.WithClientName("probe-v1"),
		probe.WithWaitForAttach(),
	)
	require.NoError(t, err)

	var sourceID string
	require.Eventually(t, func() bool {
		resp, err := http.Get(fixture.baseURL + "/api/sources")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var payload struct {
			Sources []struct {
				SourceID   string `json:"source_id"`
				ClientName string `json:"client_name"`
			} `json:"sources"`
		}
		json.NewDecoder(resp.Body).Decode(&payload)
		if len(payload.Sources) > 0 {
			sourceID = payload.Sources[0].SourceID
			return true
		}
		return false
	}, 5*time.Second, 200*time.Millisecond)

	// Close the first instrumented host (also closes base1).
	ih1.Close()
	time.Sleep(500 * time.Millisecond)

	// Create a new host with the same identity so peer_id matches.
	base2, err := libp2p.New(libp2p.Identity(priv))
	require.NoError(t, err)

	ih2, err := probe.Wrap(base2,
		probe.WithUnixSocket(fixture.socketPath),
		probe.WithClientName("probe-v2"),
		probe.WithWaitForAttach(),
	)
	require.NoError(t, err)
	defer ih2.Close()

	// The source should still have the same source_id but updated client_name.
	require.Eventually(t, func() bool {
		resp, err := http.Get(fixture.baseURL + "/api/sources")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var payload struct {
			Sources []struct {
				SourceID   string `json:"source_id"`
				ClientName string `json:"client_name"`
			} `json:"sources"`
		}
		json.NewDecoder(resp.Body).Decode(&payload)
		for _, s := range payload.Sources {
			if s.SourceID == sourceID && s.ClientName == "probe-v2" {
				return true
			}
		}
		return false
	}, 5*time.Second, 200*time.Millisecond, "expected source to be updated with probe-v2 client name")
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
	processor := backend.NewProcessor(clock)
	registry := backend.NewSourceRegistry()
	ingestListener := backend.NewIngestListener(processor, registry, nil)

	ingestCtx, ingestCancel := context.WithCancel(ctx)
	t.Cleanup(ingestCancel)
	go func() {
		_ = ingestListener.ListenAndServe(ingestCtx, socketPath)
	}()

	// Brief pause so the listener binds before the probe dials.
	time.Sleep(50 * time.Millisecond)

	server := backend.NewServer(processor, registry, nil)
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

func fetchSlotSummaries(t *testing.T, baseURL string) []backend.SlotSummary {
	t.Helper()

	resp, err := http.Get(baseURL + "/api/slots")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var payload struct {
		Slots []backend.SlotSummary `json:"slots"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	return payload.Slots
}

func fetchSlotDetail(t *testing.T, baseURL string, slot uint64) (backend.SlotDetail, int) {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("%s/api/slots/%d", baseURL, slot))
	require.NoError(t, err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return backend.SlotDetail{}, resp.StatusCode
	}

	var detail backend.SlotDetail
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
		// Server batches updates as slot_batch with a Slots array.
		if msg.Type == "slot_batch" && len(msg.Slots) > 0 {
			msg.Slot = &msg.Slots[0]
			return msg
		}
	}
	t.Fatal("timed out waiting for slot update")
	return liveMessage{}
}
