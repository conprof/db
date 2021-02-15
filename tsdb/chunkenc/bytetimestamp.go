package chunkenc

type ValueChunk struct {
	b []byte
}

// ValueChunk needs everything the ByteChunk does except timestamps. The ValueIterator should just return []byte like the TimestampChunk just returns timestamps. The appender should just add the []byte as they are passed, no compression etc. (yet).

// BytesTimestampChunk combines the BytesChunk and TimestampDoubleDeltaChunk.
// For the most part it simply relies on BytesChunk, however the Appender appends to both chunks.
// Additionally the BytesTimestampChunk has a TimestampIterator
// that allows for faster access to the timestamps only.
type BytesTimestampChunk struct {
	// BytesChunk needs to be replaced entirely with ValueChunk
	bc *BytesChunk
	tc *TimestampDoubleDeltaChunk

	// is the chunk mmaped or still open for appending? (maybe this should just be an entirely separate chunk implementation, for now we can combine but might want to split this eventually)
	immutable bool
	// contains mmaped bytes if that's now the chunk is used
	b []byte
}

func NewBytesTimestampsChunk() *BytesTimestampChunk {
	return &BytesTimestampChunk{
		bc:        NewBytesChunk(),
		tc:        NewTimestampDoubleDeltaChunk(),
		immutable: false,
	}
}

func (b *BytesTimestampChunk) Bytes() []byte {
	// if not immutable
	// create []byte that has the following:
	//   * len of timestamp chunk
	//   * timestamp chunk
	//   * len of value chunk
	//   * value chunk
	// if immutable
	//   return b.b
	return b.bc.Bytes()
}

func (b *BytesTimestampChunk) Encoding() Encoding {
	// needs new encoding
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
	// needs need iterator implementation that uses ValueIterator and TimestampIterator internally
	return b.bc.Iterator(iterator)
}

func (b *BytesTimestampChunk) NumSamples() int {
	// return timestamp chunk's number of samples (appends should happen in a way where the value is appended to ValueChunk first and then to TimestampChunk, which creates sort of a "light" transaction for that sample)
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
	// Good start, just replace with ValueAppender
	ba Appender
	ts TimestampAppender
}

func (b *BytesTimestampAppender) Append(t int64, v []byte) {
	b.ba.Append(t, v)
	b.ts.Append(t)
}
