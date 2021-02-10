package chunkenc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTimestampDoubleDeltaChunk(t *testing.T) {
	c := NewTimestampDoubleDeltaChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	for i := 0; i < 10000; i++ {
		app.Append(int64(i))
	}

	// First two full samples and rest 1 byte each.
	require.Equal(t, 10002, len(c.Bytes()))

	b := make([]byte, len(c.Bytes()))
	copy(b, c.Bytes())

	c = &TimestampDoubleDeltaChunk{
		b: b,
	}

	it := c.Iterator(nil)
	for i := int64(0); i < 10000; i++ {
		require.True(t, it.Next())
		ts := it.At()
		require.Equal(t, i, ts)
	}
}
