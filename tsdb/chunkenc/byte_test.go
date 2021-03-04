package chunkenc

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadBytesChunk(t *testing.T) {
	//c := NewBytesChunk()
	//app, _ := c.Appender()
	//app.Append(0, []byte("conprof"))
	//app.Append(1, []byte("conprof"))
	//app.Append(2, []byte("conprof"))
	//fmt.Println(c.Bytes())

	bytes := []byte{
		0, 3, // numSamples
		0, 0, 0, 3, // timestampChunk len
		0, 0, 0, 28, // valueChunk len
		0, 1, 0, // timestampChunk
		40, 181, 47, 253, 4, 0, 125, 0, 0, 64, 7, 99, 111, 110, 112, 114, 111, 102, 1, 84, 8, 3, 13, 11, 229, 122, 36, 130, // valueChunk compressed
	}

	// Create new BytesChunk with previous bytes
	c := LoadBytesChunk(bytes)

	cbytes, err := c.Bytes()
	require.NoError(t, err)
	require.Equal(t, bytes, cbytes)
	require.Equal(t, EncBytes, c.Encoding())

	tcBytes, err := c.tc.Bytes()
	require.NoError(t, err)
	require.Equal(t, []byte{0, 1, 0}, tcBytes)
	require.Equal(t, 3, c.tc.NumSamples())

	require.Equal(t, []byte{40, 181, 47, 253, 4, 0, 125, 0, 0, 64, 7, 99, 111, 110, 112, 114, 111, 102, 1, 84, 8, 3, 13, 11, 229, 122, 36, 130}, c.vc.compressed)
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

	require.Len(t, c.tc.b, 10)
	require.Len(t, c.vc.b, 80)
	require.Len(t, c.b, 0) // Isn't populated yet

	bytes, err := c.Bytes()
	require.NoError(t, err)
	require.Len(t, bytes, 48) // 2 (numSamples) + 2*4 (two chunk length) + 10+28 (chunks)

	numSamples := binary.BigEndian.Uint16(bytes[0:])
	tLen := binary.BigEndian.Uint32(bytes[2:])
	vLen := binary.BigEndian.Uint32(bytes[6:])

	require.Equal(t, uint16(total), numSamples)
	require.Equal(t, uint32(10), tLen)
	require.Equal(t, uint32(28), vLen)
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

	bytes, err := c.Bytes()
	require.NoError(t, err)

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
