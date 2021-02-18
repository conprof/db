package tsdb

import (
	"testing"

	"github.com/conprof/db/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

func TestTimestampChunk(t *testing.T) {
	c := chunkenc.NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)
	app.Append(0, []byte("abc"))
	app.Append(1, []byte("abc"))
	app.Append(2, []byte("abc"))
	app.Append(3, []byte("abc"))
	app.Append(4, []byte("abc"))
	app.Append(5, []byte("abc"))
	app.Append(6, []byte("abc"))
	app.Append(7, []byte("abc"))
	app.Append(8, []byte("abc"))
	app.Append(9, []byte("abc"))

	b := c.Bytes()
	cb, err := chunkenc.FromData(chunkenc.EncBytes, b)
	require.NoError(t, err)

	tc := &TimestampChunk{cb}
	it := tc.Iterator(nil)
	i := int64(0)
	for it.Next() {
		ti, by := it.At()
		require.Equal(t, i, ti)
		require.Equal(t, []byte(nil), by)
		i++
	}
	require.NoError(t, it.Err())
	require.Equal(t, int64(10), i)
}

func TestReencodeChunk(t *testing.T) {
	c := chunkenc.NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)
	app.Append(0, []byte("abc"))
	app.Append(1, []byte("abc"))
	app.Append(2, []byte("abc"))
	app.Append(3, []byte("abc"))
	app.Append(4, []byte("abc"))
	app.Append(5, []byte("abc"))
	app.Append(6, []byte("abc"))
	app.Append(7, []byte("abc"))
	app.Append(8, []byte("abc"))
	app.Append(9, []byte("abc"))

	b := c.Bytes()
	cb, err := chunkenc.FromData(chunkenc.EncBytes, b)
	require.NoError(t, err)

	tc := &TimestampChunk{cb}
	it := tc.Iterator(nil)

	_, newChunk, err := ReencodeChunk(tc, it)
	require.NoError(t, err)
	require.Greater(t, len(b), len(newChunk.Bytes()))
}
