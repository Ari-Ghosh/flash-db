package types

import (
	"fmt"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

// SnappyCompressor implements the Compressor interface using Snappy.
type SnappyCompressor struct{}

func (s SnappyCompressor) Compress(dst, src []byte) ([]byte, error) {
	return snappy.Encode(dst, src), nil
}

func (s SnappyCompressor) Decompress(dst, src []byte) ([]byte, error) {
	return snappy.Decode(dst, src)
}

func (s SnappyCompressor) Codec() Codec {
	return CodecSnappy
}

// ZstdCompressor implements the Compressor interface using Zstd.
type ZstdCompressor struct{}

var (
	//nolint:gochecknoglobals
	zstdEncoderOnce sync.Once
	//nolint:gochecknoglobals
	zstdDecoderOnce sync.Once
	//nolint:gochecknoglobals
	globalEncoder *zstd.Encoder
	//nolint:gochecknoglobals
	globalDecoder *zstd.Decoder
)

func getZstdEncoder() *zstd.Encoder {
	zstdEncoderOnce.Do(func() {
		var err error
		globalEncoder, err = zstd.NewWriter(nil)
		if err != nil {
			panic(fmt.Sprintf("failed to create zstd encoder: %v", err))
		}
	})
	return globalEncoder
}

func getZstdDecoder() *zstd.Decoder {
	zstdDecoderOnce.Do(func() {
		var err error
		globalDecoder, err = zstd.NewReader(nil)
		if err != nil {
			panic(fmt.Sprintf("failed to create zstd decoder: %v", err))
		}
	})
	return globalDecoder
}

func (z ZstdCompressor) Compress(dst, src []byte) ([]byte, error) {
	return getZstdEncoder().EncodeAll(src, dst), nil
}

func (z ZstdCompressor) Decompress(dst, src []byte) ([]byte, error) {
	return getZstdDecoder().DecodeAll(src, dst)
}

func (z ZstdCompressor) Codec() Codec {
	return CodecZstd
}

// NewCompressor returns a Compressor for the given Codec.
func NewCompressor(codec Codec) Compressor {
	switch codec {
	case CodecNone:
		return NoopCompressor{}
	case CodecSnappy:
		return SnappyCompressor{}
	case CodecZstd:
		return ZstdCompressor{}
	default:
		return NoopCompressor{}
	}
}
