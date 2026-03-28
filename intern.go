package instrument

import (
	"sync"

	pb "github.com/ethp2p/instrument/pb"
)

// StringInterner assigns compact sequential IDs to strings for wire efficiency.
// It uses a RWMutex so the common "already interned" path (after warmup) is
// non-exclusive across concurrent callers.
type StringInterner struct {
	mu        sync.RWMutex
	strings   []string
	stringIDs map[string]uint32
	emitter   *Emitter
}

func newStringInterner(emitter *Emitter) *StringInterner {
	return &StringInterner{
		stringIDs: make(map[string]uint32),
		emitter:   emitter,
	}
}

// Intern returns the ID for a string, emitting a StringDef event if new.
func (si *StringInterner) Intern(s string) uint32 {
	// Fast path: already interned.
	si.mu.RLock()
	if id, ok := si.stringIDs[s]; ok {
		si.mu.RUnlock()
		return id
	}
	si.mu.RUnlock()

	// Slow path: new string.
	si.mu.Lock()
	if id, ok := si.stringIDs[s]; ok {
		si.mu.Unlock()
		return id
	}
	id := uint32(len(si.strings))
	si.strings = append(si.strings, s)
	si.stringIDs[s] = id
	si.mu.Unlock()

	si.emitter.Emit(&pb.TraceEvent{
		Event: &pb.TraceEvent_StringDef{
			StringDef: &pb.StringDef{Id: id, Value: s},
		},
	})
	if si.emitter.ingestSink != nil {
		si.emitter.ingestSink.EmitStringDef(id, s)
	}
	return id
}

// snapshot returns a copy of all interned strings. Must be called externally
// synchronized if consistency with other state is needed.
func (si *StringInterner) snapshot() []string {
	si.mu.RLock()
	defer si.mu.RUnlock()
	out := make([]string, len(si.strings))
	copy(out, si.strings)
	return out
}
