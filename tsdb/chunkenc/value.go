// Copyright 2021 The Conprof Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkenc

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/klauspost/compress/zstd"
)

// zstdFrameMagic is the magic beginning of all zstd compressed frames.
// Taken from https://github.com/klauspost/compress/blob/063ee1dad7a10b7caef0432d813ec3b8d72c8a8f/zstd/framedec.go#L59
// As per standard in https://www.rfc-editor.org/rfc/rfc8478.html#section-3.1.1
var zstdFrameMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

// valueChunk needs everything the ByteChunk does except timestamps.
// The ValueIterator should just return []byte like the timestampChunk just returns timestamps.

type valueChunk struct {
	compressed []byte // only read once into b to decompress
	b          []byte // appended to by Appender and decompressed to by Iterator if compressed before
	num        uint16
}

func newValueChunk() *valueChunk {
	return &valueChunk{b: make([]byte, 0, 5000)}
}

func (c *valueChunk) Bytes() ([]byte, error) {
	if c.compressed != nil {
		return c.compressed, nil
	}

	// All samples of the chunk are uncompressed in c.b
	// Before we return these []byte we compress them with zstd.
	compressed := &bytes.Buffer{}
	encoder, err := zstd.NewWriter(compressed, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(encoder, bytes.NewBuffer(c.b))
	if err != nil {
		return nil, err
	}
	err = encoder.Close()
	if err != nil {
		return nil, err
	}
	c.compressed = compressed.Bytes()
	return c.compressed, nil
}

func (c *valueChunk) Encoding() Encoding {
	return EncValues
}

func (c *valueChunk) NumSamples() int {
	return int(c.num)
}

func (c *valueChunk) Compact() {
	if l := len(c.b); cap(c.b) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b)
		c.b = buf
	}
}

func (c *valueChunk) Appender() (*valueAppender, error) {
	return &valueAppender{
		c: c,
	}, nil
}

type valueAppender struct {
	c *valueChunk
}

func (a *valueAppender) Append(_ int64, v []byte) {
	if len(v) == 0 {
		v = []byte(" ")
	}

	buf := make([]byte, binary.MaxVarintLen64)
	size := buf[:binary.PutUvarint(buf, uint64(len(v)))]

	a.c.b = append(a.c.b, size...)
	a.c.b = append(a.c.b, v...)
	a.c.num++
	if len(a.c.compressed) != 0 {
		a.c.compressed = nil // invalidate compressed bytes after append happened
	}
}

func (c *valueChunk) Iterator(it Iterator) *valueIterator {
	if valueIter, ok := it.(*valueIterator); ok {
		//TODO: valueIter.Reset(c.b)
		return valueIter
	}

	vit := &valueIterator{
		numTotal: c.num,
	}

	// If we haven't decompressed and compressed bytes start with zstd magic number.
	if len(c.b) == 0 && len(c.compressed) != 0 && bytes.HasPrefix(c.compressed, zstdFrameMagic) {
		dec, err := zstd.NewReader(nil)
		if err != nil {
			vit.err = err
		}
		defer dec.Close()
		err = dec.Reset(bytes.NewBuffer(c.compressed))
		if err != nil {
			vit.err = err
		}
		out := &bytes.Buffer{}
		_, err = io.Copy(out, dec)
		if err != nil {
			vit.err = err
		}
		c.b = out.Bytes()
		c.compressed = nil
	}

	vit.br = bytes.NewReader(c.b)
	return vit
}

type valueIterator struct {
	br       *bytes.Reader
	numTotal uint16
	err      error

	v       []byte
	numRead uint16
}

func (it *valueIterator) Next() bool {
	if it.err != nil || it.numRead == it.numTotal {
		return false
	}

	sampleLen, err := binary.ReadUvarint(it.br)
	if err != nil {
		it.err = err
		return false
	}

	it.v = make([]byte, sampleLen)
	_, err = it.br.Read(it.v)
	if err != nil {
		it.err = err
		return false
	}

	if bytes.Equal(it.v, []byte(" ")) {
		it.v = nil
	}

	it.numRead++
	return true
}

func (it *valueIterator) Seek(_ int64) bool {
	// TODO:
	// This is interesting. We don't know anything about timestamps here.
	// We could somehow translate timestamp to index?
	// We don't need this at all?
	panic("implement me")
}

func (it *valueIterator) At() (int64, []byte) {
	// timestamp is always 0 as ignored in this chunk
	return 0, it.v
}

func (it *valueIterator) Err() error {
	return it.err
}
