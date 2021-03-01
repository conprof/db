package chunkenc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueChunk(t *testing.T) {
	c := newValueChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	v := []byte("conprof")
	for i := 0; i < 10000; i++ {
		app.Append(0, v)
	}

	compressed := c.Bytes()
	require.Equal(t, 43, len(compressed))
	num := c.NumSamples()
	require.Equal(t, 10000, num)

	c = newValueChunk()
	c.compressed = compressed
	c.num = uint16(num)

	it := c.Iterator(nil)

	for i := 0; i < 10000; i++ {
		require.True(t, it.Next())
		_, v := it.At()
		require.Equal(t, []byte("conprof"), v)
	}
}
