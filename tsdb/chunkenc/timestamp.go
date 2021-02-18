// Copyright 2017 The Prometheus Authors
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

// The code in this file was largely written by Damian Gryski as part of
// https://github.com/dgryski/go-tsz and published under the license below.
// It was modified to accommodate reading from byte slices without modifying
// the underlying bytes, which would panic when reading from mmaped
// read-only byte slices.

// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package chunkenc

import (
	"bytes"
	"encoding/binary"
	"math"
)

// timestampChunk holds only timestamps encoded with double delta.
type timestampChunk struct {
	b   []byte
	num uint16
}

// newTimestampChunk returns a new chunk with Timestamp encoding of the given size.
func newTimestampChunk() *timestampChunk {
	// Each chunk holds around 120 samples.
	// 2 bytes are used for the Samples count.
	// All timestamps occupy around 130-150 bytes leaving 4850bytes for the samples.
	// This is around 40bytes per sample.
	// If the appended samples require more space can increase this array size.
	b := make([]byte, 0, 5000)
	return &timestampChunk{b: b, num: 0}
}

// Encoding returns the encoding type.
func (c *timestampChunk) Encoding() Encoding {
	return EncTimestamps
}

// Bytes returns the underlying byte slice of the chunk.
func (c *timestampChunk) Bytes() []byte {
	return c.b
}

// NumSamples returns the number of samples in the chunk.
func (c *timestampChunk) NumSamples() int {
	return int(c.num)
}

func (c *timestampChunk) Compact() {
	if l := len(c.b); cap(c.b) > l+chunkCompactCapacityThreshold {
		buf := make([]byte, l)
		copy(buf, c.b)
		c.b = buf
	}
}

// Appender implements the Chunk interface.
func (c *timestampChunk) Appender() (*timestampAppender, error) {
	it := c.iterator(nil)

	// To get an appender we must know the state it would have if we had
	// appended all existing data from scratch.
	// We iterate through the end and populate via the iterator's state.
	for it.Next() {
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	a := &timestampAppender{
		b:      c,
		t:      it.t,
		tDelta: it.tDelta,
	}
	return a, nil
}

func (c *timestampChunk) iterator(it Iterator) *timestampsIterator {
	// Should iterators guarantee to act on a copy of the data so it doesn't lock append?
	// When using striped locks to guard access to chunks, probably yes.
	// Could only copy data if the chunk is not completed yet.
	if bytesIter, ok := it.(*timestampsIterator); ok {
		bytesIter.Reset(c.b)
		return bytesIter
	}
	return &timestampsIterator{
		br:       bytes.NewReader(c.b),
		numTotal: c.num,
		t:        math.MinInt64,
	}
}

// Iterator implements the Chunk interface.
func (c *timestampChunk) Iterator(it Iterator) *timestampsIterator {
	return c.iterator(it)
}

type timestampAppender struct {
	b *timestampChunk

	t      int64
	tDelta uint64
}

func (a *timestampAppender) Append(t int64, _ []byte) {
	var tDelta uint64
	var tt uint64

	if a.b.num == 0 {
		tt = uint64(t)

	} else if a.b.num == 1 {
		tDelta = uint64(t - a.t)
		tt = tDelta

	} else {
		tDelta = uint64(t - a.t)
		tt = tDelta - a.tDelta
	}

	// Append the time.
	buf := make([]byte, binary.MaxVarintLen64)
	time := buf[:binary.PutUvarint(buf, tt)]
	a.b.b = append(a.b.b, time...)

	a.t = t
	a.tDelta = tDelta
	a.b.num++
}

type timestampsIterator struct {
	br       *bytes.Reader
	numTotal uint16
	numRead  uint16

	t int64

	tDelta uint64
	err    error
}

func (it *timestampsIterator) Seek(t int64) bool {
	if it.err != nil {
		return false
	}

	for t > it.t || it.numRead == 0 {
		if !it.Next() {
			return false
		}
	}
	return true
}

func (it *timestampsIterator) At() (int64, []byte) {
	// We always return nil as bytes aren't stored in this chunk.
	return it.t, nil
}

func (it *timestampsIterator) Reset(b []byte) {
	it.br = bytes.NewReader(b)
	it.t = math.MinInt64
	it.numRead = 0
	it.t = 0
	it.tDelta = 0
	it.err = nil
}

func (it *timestampsIterator) Err() error {
	return it.err
}

func (it *timestampsIterator) Next() bool {
	if it.err != nil || it.numRead == it.numTotal {
		return false
	}
	t, err := binary.ReadUvarint(it.br)
	if err != nil {
		it.err = err
		return false
	}

	if it.numRead == 0 {
		it.t = int64(t)
	} else if it.numRead == 1 {
		it.tDelta = t
		it.t = it.t + int64(it.tDelta)
	} else {
		it.tDelta = uint64(int64(it.tDelta) + int64(t))
		it.t = it.t + int64(it.tDelta)
	}

	it.numRead++
	return true
}
