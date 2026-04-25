package eth

import "time"

const (
	// DefaultSlotsPerEpoch is the Ethereum consensus default.
	DefaultSlotsPerEpoch = uint64(32)
)

// SlotRef describes the wall-clock slot context for an observation.
type SlotRef struct {
	Slot         uint64
	Epoch        uint64
	SlotStart    time.Time
	SlotEnd      time.Time
	Offset       time.Duration
	OffsetMillis int64
}

// SlotClock maps wall-clock timestamps to beacon chain slots.
type SlotClock struct {
	GenesisTime    time.Time
	SecondsPerSlot uint64
	SlotsPerEpoch  uint64
}

// NewSlotClock builds a slot clock with sane defaults.
func NewSlotClock(genesis time.Time, secondsPerSlot uint64) SlotClock {
	return SlotClock{
		GenesisTime:    genesis,
		SecondsPerSlot: secondsPerSlot,
		SlotsPerEpoch:  DefaultSlotsPerEpoch,
	}
}

// At resolves slot context for the provided timestamp.
func (c SlotClock) At(t time.Time) (SlotRef, bool) {
	if c.SecondsPerSlot == 0 {
		return SlotRef{}, false
	}

	if c.SlotsPerEpoch == 0 {
		c.SlotsPerEpoch = DefaultSlotsPerEpoch
	}

	if t.Before(c.GenesisTime) {
		return SlotRef{}, false
	}

	slotDuration := time.Duration(c.SecondsPerSlot) * time.Second
	elapsed := t.Sub(c.GenesisTime)
	slot := uint64(elapsed / slotDuration)
	offset := elapsed % slotDuration
	slotStart := c.GenesisTime.Add(time.Duration(slot) * slotDuration)

	return SlotRef{
		Slot:         slot,
		Epoch:        slot / c.SlotsPerEpoch,
		SlotStart:    slotStart,
		SlotEnd:      slotStart.Add(slotDuration),
		Offset:       offset,
		OffsetMillis: offset.Milliseconds(),
	}, true
}
