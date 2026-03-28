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

type Processor struct {
	mu sync.RWMutex

	clock eth.SlotClock

	strings map[uint32]string
	streams map[uint64]*streamState
	slots   map[uint64]*slotAggregate

	onUpdate func(SlotSummary, uint64)
}

func NewProcessor(clock eth.SlotClock) *Processor {
	return &Processor{
		clock:   clock,
		strings: make(map[uint32]string),
		streams: make(map[uint64]*streamState),
		slots:   make(map[uint64]*slotAggregate),
	}
}

func (p *Processor) SetOnUpdate(fn func(SlotSummary, uint64)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onUpdate = fn
}

func (p *Processor) Apply(event *ingestpb.Envelope) {
	if event == nil {
		return
	}

	switch payload := event.Payload.(type) {
	case *ingestpb.Envelope_StringDef:
		p.mu.Lock()
		p.strings[payload.StringDef.Id] = payload.StringDef.Value
		p.mu.Unlock()
	case *ingestpb.Envelope_StreamUpsert:
		p.handleStreamUpsert(payload.StreamUpsert)
	case *ingestpb.Envelope_StreamClosed:
		p.handleStreamClosed(payload.StreamClosed)
	case *ingestpb.Envelope_StreamChunk:
		p.handleStreamChunk(event.ObservedAtNs, payload.StreamChunk)
	}
}

func (p *Processor) ListSlots(search string, limit int) []SlotSummary {
	p.mu.RLock()
	defer p.mu.RUnlock()

	currentSlot := p.currentSlotLocked()
	rows := make([]SlotSummary, 0, len(p.slots))
	search = strings.TrimSpace(strings.ToLower(search))
	for _, agg := range p.slots {
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

func (p *Processor) SlotDetail(slot uint64, protocol, topic, messageKind string) (SlotDetail, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	agg := p.slots[slot]
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
			if protocol != "" && bb.Protocol != protocol {
				continue
			}
			if topic != "" && bb.Topic != topic {
				continue
			}
			if messageKind != "" && bb.MessageKind != messageKind {
				continue
			}
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
		if protocol != "" && row.Protocol != protocol {
			continue
		}
		if topic != "" && row.Topic != topic {
			continue
		}
		if messageKind != "" && row.MessageKind != messageKind {
			continue
		}
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

func (p *Processor) CurrentSlot() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.currentSlotLocked()
}

func (p *Processor) handleStreamUpsert(stream *ingestpb.StreamUpsert) {
	if stream == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	protocol := p.strings[stream.ProtocolId]
	p.streams[stream.StreamAlias] = &streamState{
		protocol: protocol,
		decoder:  newEthStreamDecoder(protocol),
	}
}

func (p *Processor) handleStreamClosed(stream *ingestpb.StreamClosed) {
	if stream == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.streams, stream.StreamAlias)
}

func (p *Processor) handleStreamChunk(observedAtNs int64, chunk *ingestpb.StreamChunk) {
	if chunk == nil {
		return
	}

	p.mu.Lock()
	state := p.streams[chunk.StreamAlias]
	if state == nil {
		p.mu.Unlock()
		return
	}

	ref, ok := p.clock.At(time.Unix(0, observedAtNs))
	if !ok {
		p.mu.Unlock()
		return
	}

	agg := p.ensureSlotLocked(ref)
	bucketOffset := (ref.OffsetMillis / slotBucketWidthMs) * slotBucketWidthMs
	p.addRawTrafficLocked(agg, observedAtNs, ref.OffsetMillis, chunk.Direction, uint64(len(chunk.Data)))

	updatedSummary := agg.summary
	currentSlot := ref.Slot
	onUpdate := p.onUpdate

	if state.decoder == nil {
		p.addBreakdownLocked(agg, bucketOffset, chunk.Direction, uint64(len(chunk.Data)), state.protocol, "", "raw", 0)
		updatedSummary = agg.summary
		p.mu.Unlock()
		if onUpdate != nil {
			onUpdate(updatedSummary, currentSlot)
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

		p.addBreakdownLocked(agg, bucketOffset, chunk.Direction, uint64(wireBytes), state.protocol, topic, msgKind, bleedDistance)

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

		// Propagate per-slot metadata from beacon_block PUBLISH on ingress
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
		p.addBreakdownLocked(agg, bucketOffset, chunk.Direction, uint64(len(chunk.Data)), state.protocol, "", "decode_error", 0)
		state.decoder.Reset()
	}
	updatedSummary = agg.summary
	p.mu.Unlock()

	if onUpdate != nil {
		onUpdate(updatedSummary, currentSlot)
	}
}

func (p *Processor) ensureSlotLocked(ref eth.SlotRef) *slotAggregate {
	agg := p.slots[ref.Slot]
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
	p.slots[ref.Slot] = agg
	return agg
}

func (p *Processor) addRawTrafficLocked(agg *slotAggregate, observedAtNs, offsetMs int64, dir ingestpb.Direction, bytes uint64) {
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

func (p *Processor) addBreakdownLocked(agg *slotAggregate, bucketOffset int64, dir ingestpb.Direction, bytes uint64, protocol, topic, messageKind string, bleedDistance int) {
	key := slotKey{protocol: protocol, topic: topic, messageKind: messageKind}

	// Slot-level breakdown
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

	// Per-bucket breakdown
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

	// Distance bucketing
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

func summaryString(summary SlotSummary) string {
	return strings.Join([]string{
		strconv.FormatUint(summary.Slot, 10),
		strconv.FormatUint(summary.Epoch, 10),
	}, " ")
}
