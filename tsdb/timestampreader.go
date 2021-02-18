package tsdb

import "github.com/conprof/db/tsdb/chunkenc"

type chunkTimestampReader struct {
	r ChunkReader
}

func newChunkTimestampReader(r ChunkReader) ChunkReader {
	return chunkTimestampReader{
		r: r,
	}
}

func (cr chunkTimestampReader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	c, err := cr.r.Chunk(ref)
	if err != nil {
		return nil, err
	}
	return &TimestampChunk{c}, nil
}

func (cr chunkTimestampReader) Close() error { return cr.r.Close() }

type TimestampChunk struct {
	chunkenc.Chunk
}

func (c *TimestampChunk) Iterator(i chunkenc.Iterator) chunkenc.Iterator {
	it := c.Chunk.Iterator(i)
	chunkenc.ConfigureSkipValueIterator(it)

	return it
}

func ReencodeChunk(c chunkenc.Chunk, it chunkenc.Iterator) (chunkenc.Iterator, chunkenc.Chunk, error) {
	newChunk := chunkenc.NewBytesChunk()
	app, err := newChunk.Appender()
	if err != nil {
		return nil, nil, err
	}

	i := c.Iterator(it)
	for i.Next() {
		app.Append(i.At())
	}

	return i, newChunk, i.Err()
}
