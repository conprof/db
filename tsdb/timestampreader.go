package tsdb

import "github.com/conprof/db/tsdb/chunkenc"

type timestampChunkReader struct {
	r ChunkReader
}

func newTimestampChunkReader(r ChunkReader) ChunkReader {
	return timestampChunkReader{
		r: r,
	}
}

func (cr timestampChunkReader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	c, err := cr.r.Chunk(ref)
	if err != nil {
		return nil, err
	}
	return &TimestampChunk{c}, nil
}

func (cr timestampChunkReader) Close() error { return cr.Close() }

type TimestampChunk struct {
	chunkenc.Chunk
}

func (c *TimestampChunk) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	return c.Chunk.Iterator(&chunkenc.BytesTimestampOnlyIterator{})
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
