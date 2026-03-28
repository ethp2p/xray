package introspector

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Server struct {
	processor *Processor
	registry  *SourceRegistry
	storage   *Storage

	mu      sync.Mutex
	clients map[*websocket.Conn]struct{}

	pendingUpdates map[uint64]SlotSummary
	pendingCurrent uint64
	flushScheduled bool
}

func NewServer(processor *Processor, registry *SourceRegistry, storage *Storage) *Server {
	s := &Server{
		processor:      processor,
		registry:       registry,
		storage:        storage,
		clients:        make(map[*websocket.Conn]struct{}),
		pendingUpdates: make(map[uint64]SlotSummary),
	}
	processor.SetOnUpdate(s.broadcastUpdate)
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/slots", s.handleSlots)
	mux.HandleFunc("/api/slots/", s.handleSlotDetail)
	mux.HandleFunc("/api/ws", s.handleWS)
	return mux
}

func (s *Server) Serve(l net.Listener) error {
	return http.Serve(l, s.Handler())
}

func (s *Server) resolveSourceID(r *http.Request) string {
	if id := r.URL.Query().Get("source"); id != "" {
		return id
	}
	if s.registry != nil {
		if id, err := s.registry.DefaultSourceID(); err == nil {
			return id
		}
	}
	return ""
}

func (s *Server) handleSlots(w http.ResponseWriter, r *http.Request) {
	limit := 256
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	sourceID := s.resolveSourceID(r)
	writeJSON(w, map[string]any{
		"slots": s.processor.ListSlots(sourceID, r.URL.Query().Get("search"), limit),
	})
}

func (s *Server) handleSlotDetail(w http.ResponseWriter, r *http.Request) {
	slotText := strings.TrimPrefix(r.URL.Path, "/api/slots/")
	slot, err := strconv.ParseUint(slotText, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	sourceID := s.resolveSourceID(r)
	detail, ok := s.processor.SlotDetail(sourceID, slot)
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, detail)
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws upgrade failed: %v", err)
		return
	}

	// Write snapshot before registering so flushPendingUpdates cannot
	// race on this conn (gorilla/websocket forbids concurrent writers).
	_ = conn.WriteJSON(wsMessage{
		Type:    "snapshot",
		Current: s.processor.CurrentSlot(),
	})

	s.mu.Lock()
	s.clients[conn] = struct{}{}
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.clients, conn)
			s.mu.Unlock()
			_ = conn.Close()
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

// broadcastUpdate ignores sourceID for now; source scoping is added in Task 9.
func (s *Server) broadcastUpdate(sourceID string, summary SlotSummary, current uint64) {
	s.mu.Lock()
	s.pendingUpdates[summary.Slot] = summary
	if current > s.pendingCurrent {
		s.pendingCurrent = current
	}
	if !s.flushScheduled {
		s.flushScheduled = true
		time.AfterFunc(100*time.Millisecond, s.flushPendingUpdates)
	}
	s.mu.Unlock()
}

func (s *Server) flushPendingUpdates() {
	s.mu.Lock()
	clients := make([]*websocket.Conn, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	updates := make([]SlotSummary, 0, len(s.pendingUpdates))
	for _, summary := range s.pendingUpdates {
		updates = append(updates, summary)
	}
	current := s.pendingCurrent
	s.pendingUpdates = make(map[uint64]SlotSummary)
	s.pendingCurrent = 0
	s.flushScheduled = false
	s.mu.Unlock()

	if len(updates) == 0 {
		return
	}
	sort.Slice(updates, func(i, j int) bool {
		return updates[i].Slot > updates[j].Slot
	})
	s.writeToClients(clients, wsMessage{
		Type:    "slot_batch",
		Slots:   updates,
		Current: current,
	})
}

func (s *Server) writeToClients(clients []*websocket.Conn, msg wsMessage) {
	for _, client := range clients {
		if err := client.WriteJSON(msg); err != nil {
			log.Printf("ws write failed (%s): %v", client.RemoteAddr(), err)
			s.mu.Lock()
			delete(s.clients, client)
			s.mu.Unlock()
			_ = client.Close()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
