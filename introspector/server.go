package introspector

import (
	"encoding/json"
	"errors"
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

type wsClient struct {
	conn     *websocket.Conn
	sourceID string
}

type pendingKey struct {
	sourceID string
	slot     uint64
}

type Server struct {
	processor *Processor
	registry  *SourceRegistry
	storage   *Storage

	mu      sync.Mutex
	clients map[*websocket.Conn]*wsClient

	pendingUpdates map[pendingKey]SlotSummary
	pendingCurrent map[string]uint64 // sourceID -> max current slot
	flushScheduled bool
}

func NewServer(processor *Processor, registry *SourceRegistry, storage *Storage) *Server {
	s := &Server{
		processor:      processor,
		registry:       registry,
		storage:        storage,
		clients:        make(map[*websocket.Conn]*wsClient),
		pendingUpdates: make(map[pendingKey]SlotSummary),
		pendingCurrent: make(map[string]uint64),
	}
	processor.SetOnUpdate(s.broadcastUpdate)
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/slots", s.handleSlots)
	mux.HandleFunc("/api/slots/", s.handleSlotDetail)
	mux.HandleFunc("/api/sources", s.handleSources)
	mux.HandleFunc("/api/peers", s.handlePeers)
	mux.HandleFunc("/api/search", s.handleSearch)
	mux.HandleFunc("/api/ws", s.handleWS)
	return mux
}

func (s *Server) Serve(l net.Listener) error {
	return http.Serve(l, s.Handler())
}

func (s *Server) resolveSourceID(r *http.Request) (string, error) {
	if id := r.URL.Query().Get("source"); id != "" {
		return id, nil
	}
	if s.registry != nil {
		return s.registry.DefaultSourceID()
	}
	return "", errors.New("no sources available")
}

func (s *Server) handleSlots(w http.ResponseWriter, r *http.Request) {
	limit := 256
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	sourceID, err := s.resolveSourceID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	live := s.processor.ListSlots(sourceID, r.URL.Query().Get("search"), limit)

	if s.storage == nil {
		writeJSON(w, map[string]any{"slots": live})
		return
	}

	persisted := s.storage.ListSummaries(sourceID, limit)
	seen := make(map[uint64]struct{}, len(live))
	for _, slot := range live {
		seen[slot.Slot] = struct{}{}
	}
	merged := append(live, make([]SlotSummary, 0, len(persisted))...)
	for _, slot := range persisted {
		if _, ok := seen[slot.Slot]; !ok {
			merged = append(merged, slot)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Slot > merged[j].Slot })
	if len(merged) > limit {
		merged = merged[:limit]
	}

	writeJSON(w, map[string]any{"slots": merged})
}

func (s *Server) handleSlotDetail(w http.ResponseWriter, r *http.Request) {
	slotText := strings.TrimPrefix(r.URL.Path, "/api/slots/")
	slot, err := strconv.ParseUint(slotText, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	sourceID, err := s.resolveSourceID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	detail, ok := s.processor.SlotDetail(sourceID, slot)
	if !ok && s.storage != nil {
		raw, err := s.storage.ReadSlot(sourceID, slot)
		if err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Write(raw)
			return
		}
		http.NotFound(w, r)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, detail)
}

func (s *Server) handleSources(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"sources": s.registry.List()})
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	sourceID, err := s.resolveSourceID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]any{"peers": s.processor.ListPeers(sourceID)})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	sourceID, err := s.resolveSourceID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	var fromSlot, toSlot uint64
	if v := q.Get("from_slot"); v != "" {
		fromSlot, _ = strconv.ParseUint(v, 10, 64)
	}
	if v := q.Get("to_slot"); v != "" {
		toSlot, _ = strconv.ParseUint(v, 10, 64)
	}
	limit := 100
	if v := q.Get("limit"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}
	if s.storage == nil {
		writeJSON(w, map[string]any{"slots": []any{}})
		return
	}
	writeJSON(w, map[string]any{"slots": s.storage.SearchSlots(sourceID, fromSlot, toSlot, limit)})
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	sourceID, err := s.resolveSourceID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

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
	s.clients[conn] = &wsClient{conn: conn, sourceID: sourceID}
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

func (s *Server) broadcastUpdate(sourceID string, summary SlotSummary, current uint64) {
	s.mu.Lock()
	key := pendingKey{sourceID: sourceID, slot: summary.Slot}
	s.pendingUpdates[key] = summary
	if current > s.pendingCurrent[sourceID] {
		s.pendingCurrent[sourceID] = current
	}
	if !s.flushScheduled {
		s.flushScheduled = true
		time.AfterFunc(100*time.Millisecond, s.flushPendingUpdates)
	}
	s.mu.Unlock()
}

func (s *Server) flushPendingUpdates() {
	s.mu.Lock()

	type sourceUpdates struct {
		slots   []SlotSummary
		current uint64
	}
	bySource := make(map[string]*sourceUpdates)
	for key, summary := range s.pendingUpdates {
		su := bySource[key.sourceID]
		if su == nil {
			su = &sourceUpdates{}
			bySource[key.sourceID] = su
		}
		su.slots = append(su.slots, summary)
	}
	for sid, su := range bySource {
		su.current = s.pendingCurrent[sid]
	}

	clientsBySource := make(map[string][]*websocket.Conn)
	for _, wsc := range s.clients {
		clientsBySource[wsc.sourceID] = append(clientsBySource[wsc.sourceID], wsc.conn)
	}

	s.pendingUpdates = make(map[pendingKey]SlotSummary)
	s.pendingCurrent = make(map[string]uint64)
	s.flushScheduled = false
	s.mu.Unlock()

	for sid, su := range bySource {
		if len(su.slots) == 0 {
			continue
		}
		sort.Slice(su.slots, func(i, j int) bool {
			return su.slots[i].Slot > su.slots[j].Slot
		})
		msg := wsMessage{
			Type:     "slot_batch",
			SourceID: sid,
			Slots:    su.slots,
			Current:  su.current,
		}
		if pc := s.processor.PeerCount(sid); pc > 0 {
			msg.PeerCount = &pc
		}
		targets := clientsBySource[sid]
		// Also send to clients with no source filter (empty sourceID).
		if sid != "" {
			targets = append(targets, clientsBySource[""]...)
		}
		s.writeToClients(targets, msg)
	}
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
