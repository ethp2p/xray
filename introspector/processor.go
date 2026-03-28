package introspector

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethp2p/instrument/eth"
	ingestpb "github.com/ethp2p/instrument/pb/ingest"
)

const slotBucketWidthMs = 100

type slotKey struct {
	protocol    string
	topic       string
	messageKind string
}

type slotAggregate struct {
	summary         SlotSummary
	buckets         map[int64]*SlotBucketPoint
	breakdown       map[slotKey]*SlotBreakdown
	bucketBreakdown map[int64]map[slotKey]*BucketBreakdown
}

type streamState struct {
	protocol string
	decoder  StreamDecoder
}

type sourceState struct {
	strings  map[uint32]string
	streams  map[uint64]*streamState
	peers    *PeerMap
	slots    map[uint64]*slotAggregate
	lastSlot uint64
}

func newSourceState() *sourceState {
	return &sourceState{
		strings: make(map[uint32]string),
		streams: make(map[uint64]*streamState),
		peers:   NewPeerMap(),
		slots:   make(map[uint64]*slotAggregate),
	}
}

type Processor struct {
	mu    sync.RWMutex
	clock eth.SlotClock

	sources    map[string]*sourceState
	onUpdate   func(string, SlotSummary, uint64)
	onFinalize func(string, SlotDetail)
}

func NewProcessor(clock eth.SlotClock) *Processor {
	return &Processor{
		clock:   clock,
		sources: make(map[string]*sourceState),
	}
}

func (p *Processor) SetOnUpdate(fn func(string, SlotSummary, uint64)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onUpdate = fn
}

func (p *Processor) SetOnFinalize(fn func(string, SlotDetail)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onFinalize = fn
}

func (p *Processor) ApplyForSource(sourceID string, event *ingestpb.Envelope) {
	if event == nil {
		return
	}

	switch payload := event.Payload.(type) {
	case *ingestpb.Envelope_StringDef:
		p.mu.Lock()
		src := p.ensureSourceLocked(sourceID)
		src.strings[payload.StringDef.Id] = payload.StringDef.Value
		p.mu.Unlock()

	case *ingestpb.Envelope_PeerUpsert:
		p.mu.Lock()
		src := p.ensureSourceLocked(sourceID)
		src.peers.UpsertPeer(payload.PeerUpsert.PeerAlias, payload.PeerUpsert.PeerId)
		p.mu.Unlock()

	case *ingestpb.Envelope_ConnectionUpsert:
		cu := payload.ConnectionUpsert
		p.mu.Lock()
		src := p.ensureSourceLocked(sourceID)
		src.peers.UpsertConnection(cu.ConnAlias, ConnState{
			PeerAlias:  cu.PeerAlias,
			RemoteAddr: cu.RemoteAddr,
			LocalAddr:  cu.LocalAddr,
			Direction:  dirString(cu.Direction),
			Transport:  src.strings[cu.TransportId],
			Security:   src.strings[cu.SecurityId],
			Muxer:      src.strings[cu.MuxerId],
			OpenedAtNs: cu.OpenedAtNs,
		})
		p.mu.Unlock()

	case *ingestpb.Envelope_ConnectionClosed:
		p.mu.Lock()
		src := p.ensureSourceLocked(sourceID)
		src.peers.CloseConnection(payload.ConnectionClosed.ConnAlias)
		p.mu.Unlock()

	case *ingestpb.Envelope_StreamUpsert:
		p.handleStreamUpsert(sourceID, payload.StreamUpsert)

	case *ingestpb.Envelope_StreamClosed:
		p.handleStreamClosed(sourceID, payload.StreamClosed)

	case *ingestpb.Envelope_StreamChunk:
		p.handleStreamChunk(sourceID, event.ObservedAtNs, payload.StreamChunk)
	}
}

func (p *Processor) ListSlots(sourceID string, search string, limit int) []SlotSummary {
	p.mu.RLock()
	defer p.mu.RUnlock()

	src := p.sources[sourceID]
	if src == nil {
		return nil
	}

	currentSlot := p.currentSlotLocked()
	rows := make([]SlotSummary, 0, len(src.slots))
	search = strings.TrimSpace(strings.ToLower(search))
	for _, agg := range src.slots {
		summary := agg.summary
		summary.Finalized = summary.Slot < currentSlot
		if search != "" {
			slotText := strings.ToLower(strings.TrimSpace(summaryString(summary)))
			if !strings.Contains(slotText, search) {
				continue
			}
		}
		rows = append(rows, summary)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Slot > rows[j].Slot
	})
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

func (p *Processor) SlotDetail(sourceID string, slot uint64) (SlotDetail, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	src := p.sources[sourceID]
	if src == nil {
		return SlotDetail{}, false
	}

	return p.buildSlotDetailLocked(src, slot)
}

func (p *Processor) CurrentSlot() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentSlotLocked()
}

func (p *Processor) ResetSourceAliases(sourceID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	src := p.sources[sourceID]
	if src == nil {
		return
	}
	src.strings = make(map[uint32]string)
	src.streams = make(map[uint64]*streamState)
	src.peers.Clear()
}

func (p *Processor) ListPeers(sourceID string) []PeerSummary {
	p.mu.RLock()
	defer p.mu.RUnlock()

	src := p.sources[sourceID]
	if src == nil {
		return nil
	}
	return src.peers.ListPeers()
}

func (p *Processor) PeerCount(sourceID string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	src := p.sources[sourceID]
	if src == nil {
		return 0
	}
	return src.peers.Count()
}

func (p *Processor) ensureSourceLocked(sourceID string) *sourceState {
	src := p.sources[sourceID]
	if src == nil {
		src = newSourceState()
		p.sources[sourceID] = src
	}
	return src
}

func (p *Processor) handleStreamUpsert(sourceID string, stream *ingestpb.StreamUpsert) {
	if stream == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	src := p.ensureSourceLocked(sourceID)
	protocol := src.strings[stream.ProtocolId]
	src.streams[stream.StreamAlias] = &streamState{
		protocol: protocol,
		decoder:  newEthStreamDecoder(protocol),
	}
}

func (p *Processor) handleStreamClosed(sourceID string, stream *ingestpb.StreamClosed) {
	if stream == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	src := p.sources[sourceID]
	if src == nil {
		return
	}
	delete(src.streams, stream.StreamAlias)
}

func (p *Processor) handleStreamChunk(sourceID string, observedAtNs int64, chunk *ingestpb.StreamChunk) {
	if chunk == nil {
		return
	}

	p.mu.Lock()
	src := p.sources[sourceID]
	if src == nil {
		p.mu.Unlock()
		return
	}

	state := src.streams[chunk.StreamAlias]
	if state == nil {
		p.mu.Unlock()
		return
	}

	ref, ok := p.clock.At(time.Unix(0, observedAtNs))
	if !ok {
		p.mu.Unlock()
		return
	}

	currentSlot := ref.Slot

	var finalizeDetail *SlotDetail
	if src.lastSlot > 0 && currentSlot > src.lastSlot {
		if agg := src.slots[src.lastSlot]; agg != nil {
			d, _ := p.buildSlotDetailLocked(src, src.lastSlot)
			finalizeDetail = &d
		}

		if len(src.slots) > 256 {
			var oldest uint64
			for s := range src.slots {
				if oldest == 0 || s < oldest {
					oldest = s
				}
			}
			delete(src.slots, oldest)
		}
	}
	src.lastSlot = currentSlot
	onFinalize := p.onFinalize

	agg := ensureSlotLocked(src, ref)
	bucketOffset := (ref.OffsetMillis / slotBucketWidthMs) * slotBucketWidthMs
	addRawTrafficLocked(agg, observedAtNs, ref.OffsetMillis, chunk.Direction, uint64(len(chunk.Data)))

	updatedSummary := agg.summary
	onUpdate := p.onUpdate

	if state.decoder == nil {
		addBreakdownLocked(agg, bucketOffset, chunk.Direction, uint64(len(chunk.Data)), state.protocol, "", "raw", 0)
		updatedSummary = agg.summary
		p.mu.Unlock()
		if finalizeDetail != nil && onFinalize != nil {
			onFinalize(sourceID, *finalizeDetail)
		}
		if onUpdate != nil {
			onUpdate(sourceID, updatedSummary, currentSlot)
		}
		return
	}

	emit := func(wireBytes int, tags []Tag, _ any) {
		topic := tagValue(tags, tagTopic)
		msgKind := tagValue(tags, tagMessageKind)

		bleedDistance := 0
		if v := tagValue(tags, eth.TagDecodedSlot); v != "" {
			if payloadSlot, err := strconv.ParseUint(v, 10, 64); err == nil {
				if payloadSlot < ref.Slot {
					bleedDistance = int(ref.Slot - payloadSlot)
				}
			}
		}

		addBreakdownLocked(agg, bucketOffset, chunk.Direction, uint64(wireBytes), state.protocol, topic, msgKind, bleedDistance)

		if bleedDistance > 0 {
			wb := uint64(wireBytes)
			switch chunk.Direction {
			case ingestpb.Direction_DIRECTION_IN:
				if agg.summary.Meta.BleedBytesIn == nil {
					v := wb
					agg.summary.Meta.BleedBytesIn = &v
				} else {
					*agg.summary.Meta.BleedBytesIn += wb
				}
			case ingestpb.Direction_DIRECTION_OUT:
				if agg.summary.Meta.BleedBytesOut == nil {
					v := wb
					agg.summary.Meta.BleedBytesOut = &v
				} else {
					*agg.summary.Meta.BleedBytesOut += wb
				}
			}
		}

		if topic == "beacon_block" && msgKind == "PUBLISH" && chunk.Direction == ingestpb.Direction_DIRECTION_IN {
			if v := tagValue(tags, eth.TagProposerIndex); v != "" {
				if idx, err := strconv.ParseUint(v, 10, 64); err == nil {
					agg.summary.Meta.ProposerIndex = &idx
				}
			}
			if v := tagValue(tags, eth.TagAttestationCount); v != "" {
				if count, err := strconv.Atoi(v); err == nil {
					agg.summary.Meta.AttestationCount = &count
				}
			}
			if v := tagValue(tags, eth.TagBlobCommitments); v != "" {
				if count, err := strconv.Atoi(v); err == nil {
					agg.summary.Meta.BlobCommitments = &count
				}
			}
			if v := tagValue(tags, eth.TagTxCount); v != "" {
				if count, err := strconv.Atoi(v); err == nil {
					agg.summary.Meta.TxCount = &count
				}
			}
		}
	}

	var err error
	switch chunk.Direction {
	case ingestpb.Direction_DIRECTION_IN:
		err = state.decoder.ObserveRead(chunk.Data, emit)
	case ingestpb.Direction_DIRECTION_OUT:
		err = state.decoder.ObserveWrite(chunk.Data, emit)
	}
	if err != nil {
		addBreakdownLocked(agg, bucketOffset, chunk.Direction, uint64(len(chunk.Data)), state.protocol, "", "decode_error", 0)
		state.decoder.Reset()
	}
	updatedSummary = agg.summary
	p.mu.Unlock()

	if finalizeDetail != nil && onFinalize != nil {
		onFinalize(sourceID, *finalizeDetail)
	}
	if onUpdate != nil {
		onUpdate(sourceID, updatedSummary, currentSlot)
	}
}

func (p *Processor) buildSlotDetailLocked(src *sourceState, slot uint64) (SlotDetail, bool) {
	agg := src.slots[slot]
	if agg == nil {
		return SlotDetail{}, false
	}

	currentSlot := p.currentSlotLocked()
	summary := agg.summary
	summary.Finalized = summary.Slot < currentSlot

	detail := SlotDetail{Summary: summary}

	buckets := make([]SlotBucketPoint, 0, len(agg.buckets))
	for _, bucket := range agg.buckets {
		buckets = append(buckets, *bucket)
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].OffsetMs < buckets[j].OffsetMs
	})
	for i := range buckets {
		bbd := agg.bucketBreakdown[buckets[i].OffsetMs]
		if len(bbd) == 0 {
			continue
		}
		rows := make([]BucketBreakdown, 0, len(bbd))
		for _, bb := range bbd {
			rows = append(rows, *bb)
		}
		sort.Slice(rows, func(a, b int) bool {
			la := rows[a].BytesIn + rows[a].BytesOut
			lb := rows[b].BytesIn + rows[b].BytesOut
			return la > lb
		})
		buckets[i].Breakdown = rows
	}
	detail.Buckets = buckets

	breakdown := make([]SlotBreakdown, 0, len(agg.breakdown))
	for _, row := range agg.breakdown {
		breakdown = append(breakdown, *row)
	}
	sort.Slice(breakdown, func(i, j int) bool {
		left := breakdown[i].BytesIn + breakdown[i].BytesOut
		right := breakdown[j].BytesIn + breakdown[j].BytesOut
		if left == right {
			if breakdown[i].Protocol == breakdown[j].Protocol {
				if breakdown[i].Topic == breakdown[j].Topic {
					return breakdown[i].MessageKind < breakdown[j].MessageKind
				}
				return breakdown[i].Topic < breakdown[j].Topic
			}
			return breakdown[i].Protocol < breakdown[j].Protocol
		}
		return left > right
	})
	detail.Breakdown = breakdown

	return detail, true
}

func ensureSlotLocked(src *sourceState, ref eth.SlotRef) *slotAggregate {
	agg := src.slots[ref.Slot]
	if agg != nil {
		return agg
	}

	agg = &slotAggregate{
		summary: SlotSummary{
			Slot:          ref.Slot,
			Epoch:         ref.Epoch,
			SlotStartNs:   ref.SlotStart.UnixNano(),
			SlotEndNs:     ref.SlotEnd.UnixNano(),
			LastUpdatedNs: ref.SlotStart.UnixNano(),
		},
		buckets:         make(map[int64]*SlotBucketPoint),
		breakdown:       make(map[slotKey]*SlotBreakdown),
		bucketBreakdown: make(map[int64]map[slotKey]*BucketBreakdown),
	}
	src.slots[ref.Slot] = agg
	return agg
}

func addRawTrafficLocked(agg *slotAggregate, observedAtNs, offsetMs int64, dir ingestpb.Direction, bytes uint64) {
	bucketOffset := (offsetMs / slotBucketWidthMs) * slotBucketWidthMs
	point := agg.buckets[bucketOffset]
	if point == nil {
		point = &SlotBucketPoint{OffsetMs: bucketOffset}
		agg.buckets[bucketOffset] = point
	}

	switch dir {
	case ingestpb.Direction_DIRECTION_IN:
		agg.summary.BytesIn += bytes
		point.BytesIn += bytes
	case ingestpb.Direction_DIRECTION_OUT:
		agg.summary.BytesOut += bytes
		point.BytesOut += bytes
	}
	agg.summary.LastUpdatedNs = observedAtNs
}

func addBreakdownLocked(agg *slotAggregate, bucketOffset int64, dir ingestpb.Direction, bytes uint64, protocol, topic, messageKind string, bleedDistance int) {
	key := slotKey{protocol: protocol, topic: topic, messageKind: messageKind}

	row := agg.breakdown[key]
	if row == nil {
		row = &SlotBreakdown{Protocol: protocol, Topic: topic, MessageKind: messageKind}
		agg.breakdown[key] = row
	}
	row.MsgCount++
	switch dir {
	case ingestpb.Direction_DIRECTION_IN:
		row.BytesIn += bytes
		if bleedDistance > 0 {
			row.BleedBytesIn += bytes
		}
	case ingestpb.Direction_DIRECTION_OUT:
		row.BytesOut += bytes
		if bleedDistance > 0 {
			row.BleedBytesOut += bytes
		}
	}

	bbd := agg.bucketBreakdown[bucketOffset]
	if bbd == nil {
		bbd = make(map[slotKey]*BucketBreakdown)
		agg.bucketBreakdown[bucketOffset] = bbd
	}
	bb := bbd[key]
	if bb == nil {
		bb = &BucketBreakdown{Protocol: protocol, Topic: topic, MessageKind: messageKind}
		bbd[key] = bb
	}
	bb.MsgCount++
	switch dir {
	case ingestpb.Direction_DIRECTION_IN:
		bb.BytesIn += bytes
		if bleedDistance > 0 {
			bb.BleedBytesIn += bytes
		}
	case ingestpb.Direction_DIRECTION_OUT:
		bb.BytesOut += bytes
		if bleedDistance > 0 {
			bb.BleedBytesOut += bytes
		}
	}

	if bleedDistance > 0 {
		distKey := strconv.Itoa(bleedDistance)
		if bleedDistance >= 4 {
			distKey = "4+"
		}

		if row.BleedByDistance == nil {
			row.BleedByDistance = make(map[string]BleedDistEntry)
		}
		entry := row.BleedByDistance[distKey]
		switch dir {
		case ingestpb.Direction_DIRECTION_IN:
			entry.BytesIn += bytes
		case ingestpb.Direction_DIRECTION_OUT:
			entry.BytesOut += bytes
		}
		row.BleedByDistance[distKey] = entry

		if bb.BleedByDistance == nil {
			bb.BleedByDistance = make(map[string]BleedDistEntry)
		}
		bbEntry := bb.BleedByDistance[distKey]
		switch dir {
		case ingestpb.Direction_DIRECTION_IN:
			bbEntry.BytesIn += bytes
		case ingestpb.Direction_DIRECTION_OUT:
			bbEntry.BytesOut += bytes
		}
		bb.BleedByDistance[distKey] = bbEntry
	}
}

func (p *Processor) currentSlotLocked() uint64 {
	ref, ok := p.clock.At(time.Now())
	if !ok {
		return 0
	}
	return ref.Slot
}

func dirString(d ingestpb.Direction) string {
	switch d {
	case ingestpb.Direction_DIRECTION_IN:
		return "inbound"
	case ingestpb.Direction_DIRECTION_OUT:
		return "outbound"
	default:
		return "unknown"
	}
}

func summaryString(summary SlotSummary) string {
	return strings.Join([]string{
		strconv.FormatUint(summary.Slot, 10),
		strconv.FormatUint(summary.Epoch, 10),
	}, " ")
}
