package serve

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	instrument "github.com/ethp2p/instrument"
	pb "github.com/ethp2p/instrument/pb"
)

const (
	wsWriteWait      = 10 * time.Second
	wsPongWait       = 60 * time.Second
	wsPingInterval   = (wsPongWait * 9) / 10
	wsMaxMessageSize = 64 * 1024
	wsSendBuffer     = 1000
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// WsServer handles WebSocket connections for streaming.
type WsServer struct {
	emitter *instrument.Emitter
}

// NewWsServer creates a new WebSocket server.
func NewWsServer(emitter *instrument.Emitter) *WsServer {
	return &WsServer{
		emitter: emitter,
	}
}

// ServeHTTP handles WebSocket upgrade and message routing.
func (ws *WsServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	client := newWsClient(conn, ws.emitter)
	go client.writePump()
	go client.readPump()
}

type wsClient struct {
	conn    *websocket.Conn
	emitter *instrument.Emitter

	sendCh  chan *pb.ServerMessage
	closed  atomic.Bool
	dropped atomic.Uint64
}

func newWsClient(conn *websocket.Conn, emitter *instrument.Emitter) *wsClient {
	return &wsClient{
		conn:    conn,
		emitter: emitter,
		sendCh:  make(chan *pb.ServerMessage, wsSendBuffer),
	}
}

// Write implements instrument.Sink for the wsClient.
func (c *wsClient) Write(event *pb.TraceEvent) bool {
	if c.closed.Load() {
		return true
	}

	c.sendEvent(event)
	return true
}

// Close implements instrument.Sink.
func (c *wsClient) Close() error {
	c.close()
	return nil
}

func (c *wsClient) readPump() {
	defer func() {
		c.emitter.RemoveSink(c)
		c.close()
	}()

	c.conn.SetReadLimit(wsMaxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
		return nil
	})

	messageType, data, err := c.conn.ReadMessage()
	if err != nil {
		return
	}
	if messageType != websocket.BinaryMessage {
		return
	}

	handshake := &pb.Handshake{}
	if err := proto.Unmarshal(data, handshake); err != nil {
		return
	}

	c.handleHandshake(handshake)

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
	}
}

func (c *wsClient) writePump() {
	ticker := time.NewTicker(wsPingInterval)
	defer func() {
		ticker.Stop()
		c.close()
	}()

	for {
		select {
		case msg, ok := <-c.sendCh:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			data, err := proto.Marshal(msg)
			if err != nil {
				continue
			}

			if err := c.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *wsClient) handleHandshake(handshake *pb.Handshake) {
	if handshake.ResumeFromSeq > 0 {
		events := c.emitter.EventsFromSeq(handshake.ResumeFromSeq)
		if events != nil {
			for _, e := range events {
				c.sendEvent(e)
			}
		} else {
			c.sendSnapshot()
		}
	} else {
		c.sendSnapshot()
	}

	c.emitter.AddSink(c)
}

func (c *wsClient) sendSnapshot() {
	snap := c.emitter.Snapshot()
	event := &pb.TraceEvent{
		TimestampNs: time.Now().UnixNano(),
		Event:       &pb.TraceEvent_Snapshot{Snapshot: snap},
	}
	c.sendEvent(event)
}

func (c *wsClient) sendEvent(event *pb.TraceEvent) {
	if c.closed.Load() {
		return
	}

	msg := &pb.ServerMessage{
		Msg: &pb.ServerMessage_Event{Event: event},
	}

	select {
	case c.sendCh <- msg:
	default:
		n := c.dropped.Add(1)
		if n%slowWarningInterval == 0 {
			c.sendSlowWarning()
		}
	}
}

func (c *wsClient) sendSlowWarning() {
	msg := &pb.ServerMessage{
		Msg: &pb.ServerMessage_Slow{
			Slow: &pb.SlowWarning{
				BufferedEvents: uint64(len(c.sendCh)),
				DroppedEvents:  c.dropped.Load(),
			},
		},
	}
	select {
	case c.sendCh <- msg:
	default:
	}
}

func (c *wsClient) close() {
	if c.closed.Swap(true) {
		return
	}
	close(c.sendCh)
	c.conn.Close()
}
