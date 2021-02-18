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
)

// ValueChunk needs everything the ByteChunk does except timestamps.
// The ValueIterator should just return []byte like the TimestampChunk just returns timestamps.
// The appender should just add the []byte as they are passed, no compression etc. (yet).

type ValueChunk struct {
	b   []byte
	num uint16
}

func NewValueChunk() *ValueChunk {
	return &ValueChunk{b: make([]byte, 0, 5000)}
}

func (c *ValueChunk) Bytes() []byte {
	return c.b
}

func (c *ValueChunk) Encoding() Encoding {
	return EncValues
}

func (c *ValueChunk) NumSamples() int {
	return int(c.num)
}

func (c *ValueChunk) Compact() {
	if l := len(c.b); cap(c.b) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b)
		c.b = buf
	}
}

func (c *ValueChunk) Appender() (*valueAppender, error) {
	return &valueAppender{
		c: c,
	}, nil
}

type valueAppender struct {
	c *ValueChunk
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
}

func (c *ValueChunk) Iterator(it Iterator) *valueIterator {
	if valueIter, ok := it.(*valueIterator); ok {
		//TODO: valueIter.Reset(c.b)
		return valueIter
	}

	return &valueIterator{
		br:       bytes.NewReader(c.b),
		numTotal: c.num,
	}
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

func (it *valueIterator) Seek(t int64) bool {
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
