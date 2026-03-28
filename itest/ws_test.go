package itest

import (
	"context"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"

	"github.com/ethp2p/instrument"
	pb "github.com/ethp2p/instrument/pb"
	"github.com/ethp2p/instrument/serve"
)

type wsTestClient struct {
	conn   *websocket.Conn
	mu     sync.Mutex
	closed bool
	failed bool
}

func newWsTestClient(t *testing.T, addr string) *wsTestClient {
	u := url.URL{Scheme: "ws", Host: addr, Path: "/ws"}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("failed to connect to WebSocket: %v", err)
	}
	return &wsTestClient{conn: conn}
}

func (c *wsTestClient) sendHandshake(resumeFromSeq uint64) error {
	c.mu.Lock()
	if c.closed || c.failed {
		c.mu.Unlock()
		return io.EOF
	}
	c.mu.Unlock()

	data, err := proto.Marshal(&pb.Handshake{ResumeFromSeq: resumeFromSeq})
	if err != nil {
		return err
	}
	err = c.conn.WriteMessage(websocket.BinaryMessage, data)
	if err != nil {
		c.mu.Lock()
		c.failed = true
		c.mu.Unlock()
	}
	return err
}

func (c *wsTestClient) recv(timeout time.Duration) (*pb.ServerMessage, error) {
	c.mu.Lock()
	if c.closed || c.failed {
		c.mu.Unlock()
		return nil, io.EOF
	}
	c.mu.Unlock()

	c.conn.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := c.conn.ReadMessage()
	if err != nil {
		if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			c.mu.Lock()
			c.failed = true
			c.mu.Unlock()
		}
		return nil, err
	}
	msg := &pb.ServerMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (c *wsTestClient) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.conn.Close()
		c.closed = true
	}
}

func TestWs_LiveStreaming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()

	h2Raw, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer h2Raw.Close()

	h2, err := instrument.Wrap(h2Raw)
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	grpcSink := serve.NewGrpcSink()
	h2.Emitter().AddSink(grpcSink)

	connectService := serve.NewWiretapServer(h2.Emitter(), grpcSink)
	ep := serve.NewEndpoint(connectService, "127.0.0.1:0", h2.Emitter())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	defer ep.Close()
	go ep.Serve(listener)

	client := newWsTestClient(t, listener.Addr().String())
	defer client.close()

	if err := client.sendHandshake(0); err != nil {
		t.Fatal(err)
	}

	msg, err := client.recv(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if msg.GetEvent().GetSnapshot() == nil {
		t.Fatal("expected snapshot as first message")
	}

	const testProtocol = "/test/ws-live/1.0.0"

	h2.SetStreamHandler(protocol.ID(testProtocol), func(s network.Stream) {
		buf := make([]byte, 32)
		s.Read(buf)
		s.Write([]byte("pong"))
		s.Close()
	})

	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), time.Hour)
	s, err := h1.NewStream(ctx, h2.ID(), protocol.ID(testProtocol))
	if err != nil {
		t.Fatal(err)
	}
	s.Write([]byte("ping"))
	buf := make([]byte, 32)
	s.Read(buf)
	s.Close()

	var sawConnOpened, sawStreamOpened, sawTraffic bool
	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		msg, err := client.recv(200 * time.Millisecond)
		if err != nil {
			continue
		}
		event := msg.GetEvent()
		if event == nil {
			continue
		}
		if event.GetConnOpened() != nil {
			sawConnOpened = true
		}
		if event.GetStreamOpened() != nil {
			sawStreamOpened = true
		}
		if event.GetTraffic() != nil {
			sawTraffic = true
		}
		if sawConnOpened && sawStreamOpened && sawTraffic {
			break
		}
	}

	if !sawConnOpened {
		t.Error("expected ConnOpened event")
	}
	if !sawStreamOpened {
		t.Error("expected StreamOpened event")
	}
	if !sawTraffic {
		t.Error("expected Traffic event")
	}
}
