package processor

import (
	"github.com/ethp2p/xray/internal/decode"
	"github.com/ethp2p/xray/internal/eth"
)

type Tag = decode.Tag

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
	inner decode.StreamDecoder
}

func (d wrappedDecoder) ObserveRead(data []byte, emit EmitFunc) error {
	return d.inner.ObserveRead(data, func(wireBytes int, tags []decode.Tag, parsed any) {
		emit(wireBytes, tags, parsed)
	})
}

func (d wrappedDecoder) ObserveWrite(data []byte, emit EmitFunc) error {
	return d.inner.ObserveWrite(data, func(wireBytes int, tags []decode.Tag, parsed any) {
		emit(wireBytes, tags, parsed)
	})
}

func (d wrappedDecoder) Reset() {
	d.inner.Reset()
}

const (
	tagTopic       = decode.TagTopic
	tagMessageKind = decode.TagMessageKind
)
