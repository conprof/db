package chunkenc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValueChunk(t *testing.T) {
	c := NewValueChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	v := []byte("conprof")
	for i := 0; i < 10000; i++ {
		app.Append(0, v)
	}

	require.Equal(t, 80002, len(c.Bytes())) // TODO: Double check that 80002 is actually correct

	it := c.Iterator(nil)

	for i := 0; i < 10000; i++ {
		require.True(t, it.Next())
		_, v := it.At()
		require.Equal(t, []byte("conprof"), v)
	}
}
