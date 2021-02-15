package chunkenc

// BytesTimestampChunk combines the BytesChunk and TimestampDoubleDeltaChunk.
// For the most part it simply relies on BytesChunk, however the Appender appends to both chunks.
// Additionally the BytesTimestampChunk has a TimestampIterator
// that allows for faster access to the timestamps only.
type BytesTimestampChunk struct {
	bc *BytesChunk
	tc *TimestampDoubleDeltaChunk
}

func NewBytesTimestampsChunk() *BytesTimestampChunk {
	return &BytesTimestampChunk{
		bc: NewBytesChunk(),
		tc: NewTimestampDoubleDeltaChunk(),
	}
}

func (b *BytesTimestampChunk) Bytes() []byte {
	return b.bc.Bytes()
}

func (b *BytesTimestampChunk) Encoding() Encoding {
	return b.bc.Encoding()
}

func (b *BytesTimestampChunk) Appender() (Appender, error) {
	bapp, err := b.bc.Appender()
	if err != nil {
		return nil, err
	}
	tapp, err := b.tc.Appender()
	if err != nil {
		return nil, err
	}

	return &BytesTimestampAppender{
		ba: bapp,
		ts: tapp,
	}, nil
}

func (b *BytesTimestampChunk) Iterator(iterator Iterator) Iterator {
	return b.bc.Iterator(iterator)
}

func (b *BytesTimestampChunk) NumSamples() int {
	return b.bc.NumSamples()
}

func (b *BytesTimestampChunk) Compact() {
	b.bc.Compact()
	b.tc.Compact()
}

// TimestampIterator returns a TimestampIterator for faster access to the timestamps only
func (b *BytesTimestampChunk) TimestampIterator(iterator TimestampIterator) TimestampIterator {
	return b.tc.Iterator(iterator)
}

type BytesTimestampAppender struct {
	ba Appender
	ts TimestampAppender
}

func (b *BytesTimestampAppender) Append(t int64, v []byte) {
	b.ba.Append(t, v)
	b.ts.Append(t)
}
