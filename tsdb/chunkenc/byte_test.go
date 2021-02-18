package chunkenc

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadBytesChunk(t *testing.T) {
	// tree samples added (0,conprof) (1,conprof) (2,conprof)
	bytes := []byte{0, 0, 0, 5, 0, 0, 0, 26, 0, 3, 0, 1, 0, 0, 3, 7, 99, 111, 110, 112, 114, 111, 102, 7, 99, 111, 110, 112, 114, 111, 102, 7, 99, 111, 110, 112, 114, 111, 102}

	// Create new BytesChunk with previous bytes
	c := LoadBytesChunk(bytes)

	require.Equal(t, bytes, c.Bytes())
	require.Equal(t, EncBytes, c.Encoding())

	require.Equal(t, []byte{0, 3, 0, 1, 0}, c.tc.Bytes())
	require.Equal(t, 3, c.tc.NumSamples())

	require.Equal(t, []byte{0, 3, 7, 99, 111, 110, 112, 114, 111, 102, 7, 99, 111, 110, 112, 114, 111, 102, 7, 99, 111, 110, 112, 114, 111, 102}, c.vc.Bytes())
	require.Equal(t, 3, c.vc.NumSamples())
}

func TestBytesChunk_Appender(t *testing.T) {
	c := NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	total := 10

	for i := 0; i < total; i++ {
		app.Append(int64(i), []byte("conprof"))
	}

	require.Equal(t, total, c.NumSamples())
	require.Equal(t, total, c.tc.NumSamples())
	require.Equal(t, total, c.vc.NumSamples())

	require.Len(t, c.tc.b, 12)
	require.Len(t, c.vc.b, 82)
	require.Len(t, c.b, 0) // Isn't populated yet

	bytes := c.Bytes()
	require.Len(t, bytes, 102) // 12+82 (chunks) + 4*2 (two uint64)

	tLen := binary.BigEndian.Uint32(bytes[0:])
	vLen := binary.BigEndian.Uint32(bytes[4:])

	require.Equal(t, uint32(12), tLen)
	require.Equal(t, uint32(82), vLen)
}

func BenchmarkBytesChunk_Appender(b *testing.B) {
	c := NewBytesChunk()
	app, _ := c.Appender()

	b.ResetTimer()
	b.ReportAllocs()

	v := []byte("conprof")
	for i := 0; i < b.N; i++ {
		app.Append(int64(i), v)
	}
}

func TestBytesChunk_Iterator(t *testing.T) {
	c := NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	total := 10_000

	for i := 0; i < total; i++ {
		app.Append(int64(i), []byte(fmt.Sprintf("conprof-%d", i)))
	}

	require.Equal(t, total, c.NumSamples())

	it := c.Iterator(nil)
	for i := 0; i < total; i++ {
		require.True(t, it.Next())
		ts, v := it.At()
		require.Equal(t, int64(i), ts)
		require.Equal(t, []byte(fmt.Sprintf("conprof-%d", i)), v)
	}

	require.NoError(t, it.Err())
}

func TestBytesChunk_IteratorImmutable(t *testing.T) {
	c := NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	v := []byte("conprof")

	total := 10_000
	for i := 0; i < total; i++ {
		app.Append(int64(i), v)
	}

	bytes := c.Bytes()

	// Create new immutable BytesChunk
	c = LoadBytesChunk(bytes)

	it := c.Iterator(nil)
	for i := 0; i < total; i++ {
		require.True(t, it.Next())
		ts, v := it.At()
		require.Equal(t, int64(i), ts)
		require.Equal(t, v, v)
	}
}

func TestBytesTimestampValuesIterator_Seek(t *testing.T) {
	c := NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	total := 10
	for i := 0; i < total; i++ {
		app.Append(int64(i), []byte(fmt.Sprintf("conprof-%d", i)))
	}

	it := c.Iterator(nil)
	require.True(t, it.Seek(5))
	require.NoError(t, it.Err())

	ts, v := it.At()
	require.Equal(t, int64(5), ts)
	require.Equal(t, "conprof-5", string(v))
}

func BenchmarkBytesTimestampValuesIterator_Seek(b *testing.B) {
	var (
		chunks []*BytesChunk
		t      = int64(1234123324)
	)

	chunk := NewBytesChunk()
	app, err := chunk.Appender()
	require.NoError(b, err)

	j := 0
	for i := 0; i < b.N; i++ {
		if j > 250 {
			chunks = append(chunks, chunk)
			chunk = NewBytesChunk()
			app, err = chunk.Appender()
			require.NoError(b, err)
			j = 0
		}

		t += int64(100)
		v := fmt.Sprintf("conprof-%d", t)
		app.Append(t, []byte(v))

		j++
	}
	chunks = append(chunks, chunk) // append last missing chunk

	b.ReportAllocs()
	b.ResetTimer()

	// seek for last timestamp in all chunks
	for _, c := range chunks {
		it := c.Iterator(nil)
		if it.Seek(t) {
			itT, itV := it.At()
			require.Equal(b, t, itT)
			require.Equal(b, []byte(fmt.Sprintf("conprof-%d", t)), itV)
		}
	}
}

func BenchmarkBytesTimestampOnlyIterator_Seek(b *testing.B) {
	var (
		chunks []*BytesChunk
		t      = int64(1234123324)
	)

	chunk := NewBytesChunk()
	app, err := chunk.Appender()
	require.NoError(b, err)

	j := 0
	for i := 0; i < b.N; i++ {
		if j > 250 {
			chunks = append(chunks, chunk)
			chunk = NewBytesChunk()
			app, err = chunk.Appender()
			require.NoError(b, err)
			j = 0
		}

		t += int64(100)
		v := fmt.Sprintf("conprof-%d", t)
		app.Append(t, []byte(v))

		j++
	}
	chunks = append(chunks, chunk) // append last missing chunk

	b.ReportAllocs()
	b.ResetTimer()

	// seek for last timestamp in all chunks
	for _, c := range chunks {
		it := c.Iterator(&BytesTimestampOnlyIterator{})
		if it.Seek(t) {
			itT, itV := it.At()
			require.Equal(b, t, itT)
			require.Equal(b, []byte(nil), itV)
		}
	}
}
