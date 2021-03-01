package chunkenc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/require"
)

func TestLoadBytesChunk(t *testing.T) {
	//c := NewBytesChunk()
	//app, _ := c.Appender()
	//app.Append(0, []byte("conprof"))
	//app.Append(1, []byte("conprof"))
	//app.Append(2, []byte("conprof"))
	//fmt.Println(c.Bytes())

	bytes := []byte{
		0, 3, // numSamples
		0, 0, 0, 3, // timestampChunk len
		0, 0, 0, 28, // valueChunk len
		0, 1, 0, // timestampChunk
		40, 181, 47, 253, 4, 96, 125, 0, 0, 64, 7, 99, 111, 110, 112, 114, 111, 102, 1, 84, 8, 3, 13, 11, 229, 122, 36, 130, // valueChunk compressed
	}

	// Create new BytesChunk with previous bytes
	c := LoadBytesChunk(bytes)

	require.Equal(t, bytes, c.Bytes())
	require.Equal(t, EncBytes, c.Encoding())

	require.Equal(t, []byte{0, 1, 0}, c.tc.Bytes())
	require.Equal(t, 3, c.tc.NumSamples())

	require.Equal(t, []byte{40, 181, 47, 253, 4, 96, 125, 0, 0, 64, 7, 99, 111, 110, 112, 114, 111, 102, 1, 84, 8, 3, 13, 11, 229, 122, 36, 130}, c.vc.compressed)
	require.Equal(t, 3, c.vc.NumSamples())
}

func TestBytesChunk_Appender(t *testing.T) {
	c := NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	total := 10

	for i := 0; i < total; i++ {
		app.Append(int64(i), []byte("conprof"))
	}

	require.Equal(t, total, c.NumSamples())
	require.Equal(t, total, c.tc.NumSamples())
	require.Equal(t, total, c.vc.NumSamples())

	require.Len(t, c.tc.b, 10)
	require.Len(t, c.vc.b, 80)
	require.Len(t, c.b, 0) // Isn't populated yet

	bytes := c.Bytes()
	require.Len(t, bytes, 48) // 2 (numSamples) + 2*4 (two chunk length) + 10+28 (chunks)

	numSamples := binary.BigEndian.Uint16(bytes[0:])
	tLen := binary.BigEndian.Uint32(bytes[2:])
	vLen := binary.BigEndian.Uint32(bytes[6:])

	require.Equal(t, uint16(total), numSamples)
	require.Equal(t, uint32(10), tLen)
	require.Equal(t, uint32(28), vLen)
}

func BenchmarkBytesChunk_Appender(b *testing.B) {
	c := NewBytesChunk()
	app, _ := c.Appender()

	b.ResetTimer()
	b.ReportAllocs()

	v := []byte("conprof")
	for i := 0; i < b.N; i++ {
		app.Append(int64(i), v)
	}
}

func TestBytesChunk_Iterator(t *testing.T) {
	c := NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	total := 10_000

	for i := 0; i < total; i++ {
		app.Append(int64(i), []byte(fmt.Sprintf("conprof-%d", i)))
	}

	require.Equal(t, total, c.NumSamples())

	it := c.Iterator(nil)
	for i := 0; i < total; i++ {
		require.True(t, it.Next())
		ts, v := it.At()
		require.Equal(t, int64(i), ts)
		require.Equal(t, []byte(fmt.Sprintf("conprof-%d", i)), v)
	}

	require.NoError(t, it.Err())
}

func TestBytesChunk_IteratorImmutable(t *testing.T) {
	c := NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	v := []byte("conprof")

	total := 10_000
	for i := 0; i < total; i++ {
		app.Append(int64(i), v)
	}

	bytes := c.Bytes()

	// Create new immutable BytesChunk
	c = LoadBytesChunk(bytes)

	it := c.Iterator(nil)
	for i := 0; i < total; i++ {
		require.True(t, it.Next())
		ts, v := it.At()
		require.Equal(t, int64(i), ts)
		require.Equal(t, v, v)
	}
}

func TestBytesTimestampValuesIterator_Seek(t *testing.T) {
	c := NewBytesChunk()
	app, err := c.Appender()
	require.NoError(t, err)

	total := 10
	for i := 0; i < total; i++ {
		app.Append(int64(i), []byte(fmt.Sprintf("conprof-%d", i)))
	}

	it := c.Iterator(nil)
	require.True(t, it.Seek(5))
	require.NoError(t, it.Err())

	ts, v := it.At()
	require.Equal(t, int64(5), ts)
	require.Equal(t, "conprof-5", string(v))
}

func BenchmarkBytesTimestampValuesIterator_Seek(b *testing.B) {
	var (
		chunks []*BytesChunk
		t      = int64(1234123324)
	)

	chunk := NewBytesChunk()
	app, err := chunk.Appender()
	require.NoError(b, err)

	j := 0
	for i := 0; i < b.N; i++ {
		if j > 250 {
			chunks = append(chunks, chunk)
			chunk = NewBytesChunk()
			app, err = chunk.Appender()
			require.NoError(b, err)
			j = 0
		}

		t += int64(100)
		v := fmt.Sprintf("conprof-%d", t)
		app.Append(t, []byte(v))

		j++
	}
	chunks = append(chunks, chunk) // append last missing chunk

	b.ReportAllocs()
	b.ResetTimer()

	// seek for last timestamp in all chunks
	for _, c := range chunks {
		it := c.Iterator(nil)
		if it.Seek(t) {
			itT, itV := it.At()
			require.Equal(b, t, itT)
			require.Equal(b, []byte(fmt.Sprintf("conprof-%d", t)), itV)
		}
	}
}

func BenchmarkBytesTimestampOnlyIterator_Seek(b *testing.B) {
	var (
		chunks []*BytesChunk
		t      = int64(1234123324)
	)

	chunk := NewBytesChunk()
	app, err := chunk.Appender()
	require.NoError(b, err)

	j := 0
	for i := 0; i < b.N; i++ {
		if j > 250 {
			chunks = append(chunks, chunk)
			chunk = NewBytesChunk()
			app, err = chunk.Appender()
			require.NoError(b, err)
			j = 0
		}

		t += int64(100)
		v := fmt.Sprintf("conprof-%d", t)
		app.Append(t, []byte(v))

		j++
	}
	chunks = append(chunks, chunk) // append last missing chunk

	b.ReportAllocs()
	b.ResetTimer()

	// seek for last timestamp in all chunks
	for _, c := range chunks {
		it := c.Iterator(&BytesTimestampOnlyIterator{})
		if it.Seek(t) {
			itT, itV := it.At()
			require.Equal(b, t, itT)
			require.Equal(b, []byte(nil), itV)
		}
	}
}

type sample struct {
	t int64
	v []byte
}

func loadSamples() ([]sample, error) {
	filepathTimestamp, err := regexp.Compile(`(\d{13}).pb.gz$`)
	if err != nil {
		return nil, err
	}

	files, err := filepath.Glob("/home/metalmatze/Downloads/conprof/heap/*.pb.gz")
	if err != nil {
		return nil, err
	}

	samples := make([]sample, 0, len(files))
	for _, file := range files {
		submatch := filepathTimestamp.FindStringSubmatch(file)
		if len(submatch) != 2 {
			return nil, fmt.Errorf("expected 2 matches in timestamp regexp")
		}
		ts, err := strconv.Atoi(submatch[1])
		if err != nil {
			return nil, err
		}
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		r, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		v := &bytes.Buffer{}
		_, err = io.Copy(v, r)
		if err != nil {
			return nil, err
		}
		samples = append(samples, sample{t: int64(ts), v: v.Bytes()})
	}

	return samples, nil
}

func BenchmarkBytesAppender(b *testing.B) {
	//start := time.Now()
	samples, err := loadSamples()
	//b.Logf("Loaded %d real samples in %v\n", len(samples), time.Since(start))

	c := NewBytesChunk()
	app, err := c.Appender()
	require.NoError(b, err)

	sl := len(samples)
	uncompressed := 0

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s := samples[i%sl]
		app.Append(s.t, s.v)
		uncompressed += len(s.v)
	}

	compressed := len(c.Bytes())
	b.ReportMetric(float64(compressed)/float64(b.N), "bytes/sample")

	if b.N != 1 {
		b.Logf("Compressed %d samples %s into %s saving %.2f%%\n",
			b.N,
			byteSize(uncompressed),
			byteSize(compressed),
			100-100*(float64(compressed)/float64(uncompressed)),
		)
	}
}

func byteSize(s int) string {
	if s > 1024*1024*1024 {
		return fmt.Sprintf("%.2fGiB", float64(s)/1024/1024/1024)
	}
	if s > 1024*1024 {
		return fmt.Sprintf("%.2fMiB", float64(s)/1024/1024)
	}
	if s > 1024 {
		return fmt.Sprintf("%.2fKiB", float64(s)/1024)
	}
	return fmt.Sprintf("%dB", s)
}

//func TestBytesChunkLength(t *testing.T) {
//	start := time.Now()
//	samples, err := loadSamples()
//	require.NoError(t, err)
//	t.Logf("Loaded %d real samples in %v\n", len(samples), time.Since(start))
//
//	total := 1_500
//	step := 6
//
//	type result struct {
//		amount int
//		length float64
//	}
//	results := make([]result, 0, total/step)
//
//	for i := step; i <= total; i = i + step {
//		c := NewBytesChunk()
//		app, err := c.Appender()
//		require.NoError(t, err)
//
//		for j := 0; j < i; j++ {
//			s := samples[j]
//			app.Append(s.t, s.v)
//		}
//
//		results = append(results, result{
//			amount: i,
//			length: float64(len(c.Bytes())) / float64(i),
//		})
//		fmt.Println("done with", i)
//	}
//
//	for _, c := range results {
//		fmt.Printf("%d\t%.2f\n", c.amount, c.length/1024)
//	}
//}
