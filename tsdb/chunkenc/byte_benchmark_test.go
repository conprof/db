// +build benchmark

// This file contains some more specific benchmarks that one needs a benchmark dataset for and more.
// Therefore it is usually excluded unless it is enabled through the specific build tag.
package chunkenc

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type sample struct {
	t int64
	v []byte
}

func loadSamples(amount int) ([]sample, error) {
	filepathTimestamp, err := regexp.Compile(`(\d{13}).pb.gz$`)
	if err != nil {
		return nil, err
	}

	// Adjust this path to your local file system to where benchmark dataset is located.
	files, err := filepath.Glob("/home/metalmatze/Downloads/conprof/heap/*.pb.gz")
	if err != nil {
		return nil, err
	}

	if amount < 0 {
		amount = len(files)
	}

	samples := make([]sample, 0, amount)

	for _, file := range files[:amount] {
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
	samples, err := loadSamples(-1) // < 0 is all samples
	require.NoError(b, err)

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

func TestBytesChunkLength(t *testing.T) {
	total := 100
	step := total / 1 // get 100 data points

	start := time.Now()
	samples, err := loadSamples(total)
	require.NoError(t, err)
	t.Logf("Loaded %d real samples in %v\n", len(samples), time.Since(start))

	type result struct {
		amount int
		length float64
	}
	results := make([]result, 0, total/step)

	for i := step; i <= total; i = i + step {
		c := NewBytesChunk()
		app, err := c.Appender()
		require.NoError(t, err)

		for j := 0; j < i; j++ {
			s := samples[j]
			app.Append(s.t, s.v)
		}

		results = append(results, result{
			amount: i,
			length: float64(len(c.Bytes())) / float64(i),
		})
		//fmt.Println("done with", i)
	}

	for _, c := range results {
		fmt.Printf("%d,%.2f\n", c.amount, c.length/1024)
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
