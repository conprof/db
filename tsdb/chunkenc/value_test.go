package chunkenc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueChunk(t *testing.T) {
	c := newValueChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	total := 10_000
	v := []byte("conprof")
	for i := 0; i < total; i++ {
		app.Append(0, v)
	}
	require.Equal(t, total, c.NumSamples())

	compressed, err := c.Bytes()
	require.NoError(t, err)
	require.Equal(t, 43, len(compressed))

	c = newValueChunk()
	c.compressed = compressed
	c.num = uint16(total)

	it := c.Iterator(nil)
	for i := 0; i < total; i++ {
		require.True(t, it.Next(), i)
		_, v := it.At()
		require.Equal(t, []byte("conprof"), v)
	}
}
