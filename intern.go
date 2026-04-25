package xray

import (
	"sync"

	wiretappb "github.com/ethp2p/xray/proto/wiretap"
)

// stringInterner assigns compact sequential IDs to strings for wire efficiency.
// It uses a RWMutex so the common "already interned" path (after warmup) is
// non-exclusive across concurrent callers.
type stringInterner struct {
	mu        sync.RWMutex
	strings   []string
	stringIDs map[string]uint32
	emitter   *Emitter
}

func newStringInterner(emitter *Emitter) *stringInterner {
	return &stringInterner{
		stringIDs: make(map[string]uint32),
		emitter:   emitter,
	}
}

// Intern returns the ID for a string, emitting a StringDef envelope if new.
func (si *stringInterner) Intern(s string) uint32 {
	si.mu.RLock()
	if id, ok := si.stringIDs[s]; ok {
		si.mu.RUnlock()
		return id
	}
	si.mu.RUnlock()

	si.mu.Lock()
	if id, ok := si.stringIDs[s]; ok {
		si.mu.Unlock()
		return id
	}
	id := uint32(len(si.strings))
	si.strings = append(si.strings, s)
	si.stringIDs[s] = id
	si.mu.Unlock()

	si.emitter.Emit(&wiretappb.Envelope{
		Payload: &wiretappb.Envelope_StringDef{
			StringDef: &wiretappb.StringDef{Id: id, Value: s},
		},
	})
	return id
}

// snapshot returns a copy of all interned strings.
func (si *stringInterner) snapshot() []string {
	si.mu.RLock()
	defer si.mu.RUnlock()
	out := make([]string, len(si.strings))
	copy(out, si.strings)
	return out
}
