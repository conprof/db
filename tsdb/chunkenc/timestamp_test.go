package chunkenc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTimestampChunk(t *testing.T) {
	c := newTimestampChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	for i := 0; i < 10000; i++ {
		app.Append(int64(i), nil)
	}

	// First two full samples and rest 1 byte each.
	require.Equal(t, 10000, len(c.Bytes()))

	b := make([]byte, len(c.Bytes()))
	copy(b, c.Bytes())

	c = &timestampChunk{
		b:   b,
		num: c.num,
	}

	it := c.Iterator(nil)
	for i := int64(0); i < 10000; i++ {
		require.True(t, it.Next())
		ts, _ := it.At()
		require.Equal(t, i, ts)
	}
}

func TestTimestampsIterator_Seek(t *testing.T) {
	c := newTimestampChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	total := 1_000
	for i := 0; i < total; i++ {
		app.Append(int64(i), nil)
	}

	it := c.Iterator(nil)

	require.True(t, it.Seek(100))
	require.NoError(t, it.Err())
	ts, _ := it.At()
	require.Equal(t, int64(100), ts)
}
