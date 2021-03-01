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
	"encoding/binary"
)

const (
	chunkCompactCapacityThreshold = 32
)

// BytesChunk combines the valueChunk and timestampChunk.
// The Appender and Iterator work on both underlying chunks.
// The reason the BytesChunk is split up, is to allow to iterate over chunks
// and optionally disable reading values at all, when only timestamps are needed.
type BytesChunk struct {
	tc *timestampChunk
	vc *valueChunk

	b   []byte
	num uint16
}

func NewBytesChunk() *BytesChunk {
	return &BytesChunk{
		tc: newTimestampChunk(),
		vc: newValueChunk(),
	}
}

func LoadBytesChunk(b []byte) *BytesChunk {
	num := binary.BigEndian.Uint16(b[0:2])               // first 16bit
	timestampChunkLen := binary.BigEndian.Uint32(b[2:6]) // second 32bit
	valueChunkLen := binary.BigEndian.Uint32(b[6:10])    // third 32bit

	timestampChunkStart := uint32(10) // after first 16bit + two 32bit (64bit)
	timestampChunkEnd := timestampChunkStart + timestampChunkLen
	valueChunkStart := timestampChunkEnd
	valueChunkEnd := valueChunkStart + valueChunkLen

	return &BytesChunk{
		b:   b,
		num: num,
		tc:  &timestampChunk{b: b[timestampChunkStart:timestampChunkEnd], num: num},
		vc:  &valueChunk{compressed: b[valueChunkStart:valueChunkEnd], num: num},
	}
}

func (b *BytesChunk) Bytes() []byte {
	if len(b.b) > 0 {
		return b.b
	}

	dataNumSamples := make([]byte, 2)
	binary.BigEndian.PutUint16(dataNumSamples, uint16(b.NumSamples()))

	dataTimestampChunk := b.tc.Bytes()
	dataValueChunk := b.vc.Bytes()

	// We store chunk length as uint32 which allows chunks to be up to 4GiB

	dataTimestampChunkLen := make([]byte, 4)
	binary.BigEndian.PutUint32(dataTimestampChunkLen, uint32(len(dataTimestampChunk)))

	dataValueChunkLen := make([]byte, 4)
	binary.BigEndian.PutUint32(dataValueChunkLen, uint32(len(dataValueChunk)))

	// TODO: Probably better with copy()

	data := make([]byte, 0, 2+2*4+len(dataTimestampChunk)+len(dataValueChunk)) // two 32 bits of length for each chunks size and the chunks themselves
	data = append(data, dataNumSamples...)
	data = append(data, dataTimestampChunkLen...)
	data = append(data, dataValueChunkLen...)
	data = append(data, dataTimestampChunk...)
	data = append(data, dataValueChunk...)
	return data
}

func (b *BytesChunk) Encoding() Encoding {
	return EncBytes
}

func (b *BytesChunk) NumSamples() int {
	if len(b.b) == 0 {
		return int(b.tc.num)
	}
	return int(b.num)
}

func (b *BytesChunk) Compact() {
	b.tc.Compact()
	b.vc.Compact()
}

func (b *BytesChunk) Appender() (Appender, error) {
	tapp, err := b.tc.Appender()
	if err != nil {
		return nil, err
	}
	vapp, err := b.vc.Appender()
	if err != nil {
		return nil, err
	}

	return &BytesAppender{
		ta: tapp,
		va: vapp,
	}, nil
}

type BytesAppender struct {
	ta *timestampAppender
	va *valueAppender
}

func (b *BytesAppender) Append(t int64, v []byte) {
	// Both Appenders implement the Appender interface.
	// As both only care about one parameter we simply pass the zero value as the other.
	b.va.Append(0, v)
	b.ta.Append(t, nil)
}

func (b *BytesChunk) Iterator(iterator Iterator) Iterator {
	if iterator != nil {
		if it, ok := iterator.(*BytesTimestampOnlyIterator); ok {
			it.tIt = b.tc.Iterator(nil)
			return it
		}
	}

	return &BytesTimestampValuesIterator{
		tIt: b.tc.Iterator(nil),
		vIt: b.vc.Iterator(nil),
	}
}

type BytesTimestampValuesIterator struct {
	tIt      *timestampsIterator
	vIt      *valueIterator
	numTotal uint16

	numRead uint16
	err     error
	t       int64
	v       []byte
}

func (it *BytesTimestampValuesIterator) Next() bool {
	if it.tIt.err != nil {
		it.err = it.tIt.err // copy over error - good idea?
		return false
	}
	if it.vIt.err != nil {
		it.err = it.vIt.err // copy over error - good idea?
		return false
	}

	if it.tIt.Next() && it.vIt.Next() {
		it.t, _ = it.tIt.At()
		_, it.v = it.vIt.At()
		it.numRead++
		return true
	}

	return false
}

func (it *BytesTimestampValuesIterator) Seek(t int64) bool {
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

func (it *BytesTimestampValuesIterator) At() (int64, []byte) {
	var (
		t int64
		v []byte
	)
	t, _ = it.tIt.At()
	_, v = it.vIt.At()
	return t, v
}

func (it *BytesTimestampValuesIterator) Err() error {
	if err := it.tIt.Err(); err != nil {
		return err
	}
	if err := it.vIt.Err(); err != nil {
		return err
	}
	return nil
}

type BytesTimestampOnlyIterator struct {
	tIt *timestampsIterator
}

func (it *BytesTimestampOnlyIterator) Next() bool {
	return it.tIt.Next()
}

func (it *BytesTimestampOnlyIterator) Seek(t int64) bool {
	return it.tIt.Seek(t)
}

func (it *BytesTimestampOnlyIterator) At() (int64, []byte) {
	return it.tIt.At()
}

func (it *BytesTimestampOnlyIterator) Err() error {
	return it.tIt.Err()
}
