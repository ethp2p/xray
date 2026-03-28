package introspector

import (
	instrument "github.com/ethp2p/instrument"
	"github.com/ethp2p/instrument/eth"
	gs "github.com/ethp2p/instrument/libp2p/gossipsub"
)

type Tag struct {
	Name   string
	Values []string
}

type EmitFunc func(wireBytes int, tags []Tag, parsed any)

type StreamDecoder interface {
	ObserveRead(data []byte, emit EmitFunc) error
	ObserveWrite(data []byte, emit EmitFunc) error
	Reset()
}

func newEthStreamDecoder(protocol string) StreamDecoder {
	decoder := eth.GossipSubDecoder()
	if !decoder.Match(protocol) {
		return nil
	}
	return wrappedDecoder{inner: decoder.New()}
}

func tagValue(tags []Tag, name string) string {
	for _, tag := range tags {
		if tag.Name == name && len(tag.Values) > 0 {
			return tag.Values[0]
		}
	}
	return ""
}

type wrappedDecoder struct {
	inner instrument.StreamDecoder
}

func (d wrappedDecoder) ObserveRead(data []byte, emit EmitFunc) error {
	return d.inner.ObserveRead(data, instrumentEmitAdapter(emit))
}

func (d wrappedDecoder) ObserveWrite(data []byte, emit EmitFunc) error {
	return d.inner.ObserveWrite(data, instrumentEmitAdapter(emit))
}

func (d wrappedDecoder) Reset() {
	d.inner.Reset()
}

func instrumentEmitAdapter(emit EmitFunc) instrument.EmitFunc {
	return func(wireBytes int, tags []instrument.Tag, parsed any) {
		converted := make([]Tag, 0, len(tags))
		for _, tag := range tags {
			converted = append(converted, Tag{
				Name:   tag.Name,
				Values: append([]string(nil), tag.Values...),
			})
		}
		emit(wireBytes, converted, parsed)
	}
}

const (
	tagTopic       = gs.TagTopic
	tagMessageKind = gs.TagMessageKind
)
