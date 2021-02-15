package chunkenc

import (
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBytesTimestampsChunk(t *testing.T) {
	for enc, nc := range map[Encoding]func() Chunk{
		EncXOR: func() Chunk { return NewBytesTimestampsChunk() },
	} {
		t.Run(fmt.Sprintf("%v", enc), func(t *testing.T) {
			for range make([]struct{}, 1) {
				c := nc()
				if err := testBytesTimestampsChunk(c); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func testBytesTimestampsChunk(c Chunk) error {
	app, err := c.Appender()
	if err != nil {
		return err
	}

	var exp []pair
	var (
		ts = int64(1)
		v  = "a"
	)
	for i := 0; i < 10; i++ {

		// Start with a new appender every 10th sample. This emulates starting
		// appending to a partially filled chunk.
		if i%10 == 0 {
			app, err = c.Appender()
			if err != nil {
				return err
			}
		}

		app.Append(ts, []byte(v))
		exp = append(exp, pair{t: ts, v: v})

		ts += 2
		// v = rand.Float64()
		v += "a"
		// fmt.Println("appended", len(c.Bytes()), c.Bytes())
	}

	it := c.Iterator(nil)
	var res []pair
	for it.Next() {
		ts, v := it.At()
		res = append(res, pair{t: ts, v: string(v)})
	}
	if it.Err() != nil {
		return it.Err()
	}
	if !reflect.DeepEqual(exp, res) {
		return fmt.Errorf("unexpected result\n\ngot: %v\n\nexp: %v", res, exp)
	}
	return nil
}

//func BenchmarkBytesTimestampChunk_Iterator(b *testing.B) {
//	chunks := benchmarkPopulateBytesTimestampsChunk(b)
//
//	b.ReportAllocs()
//	b.ResetTimer()
//
//	b.Log("num", b.N, "created chunks", len(chunks))
//
//	res := make([][]byte, 0, 1024)
//
//	var it Iterator
//	for i := 0; i < len(chunks); i++ {
//		c := chunks[i]
//		it := c.Iterator(it)
//
//		for it.Next() {
//			_, v := it.At()
//			res = append(res, v)
//		}
//		if it.Err() != io.EOF {
//			require.NoError(b, it.Err())
//		}
//		res = res[:0]
//	}
//}

func BenchmarkBytesTimestampChunk_Iterator(b *testing.B) {
	chunks := benchmarkPopulateBytesTimestampsChunk(b)

	b.ReportAllocs()
	b.ResetTimer()

	b.Log("num", b.N, "created chunks", len(chunks))

	res := make([]int64, 0, 1024)

	var it TimestampIterator
	for i := 0; i < len(chunks); i++ {
		c := chunks[i]
		it := c.TimestampIterator(it)

		for it.Next() {
			t := it.At()
			res = append(res, t)
		}
		if it.Err() != io.EOF {
			require.NoError(b, it.Err())
		}
		res = res[:0]
	}
}

func benchmarkPopulateBytesTimestampsChunk(b *testing.B) []*BytesTimestampChunk {
	b.Helper()

	var (
		t   = int64(1234123324)
		v   = ""
		exp []pair
	)
	for i := 0; i < b.N; i++ {
		// t += int64(rand.Intn(10000) + 1)
		t += int64(1000)
		v = fmt.Sprintf("foobar%d", i)
		exp = append(exp, pair{t: t, v: v})
	}

	var chunks []*BytesTimestampChunk
	for i := 0; i < b.N; {
		c := NewBytesTimestampsChunk()

		a, err := c.Appender()
		if err != nil {
			b.Fatalf("get appender: %s", err)
		}
		j := 0
		for _, p := range exp {
			if j > 250 {
				break
			}
			a.Append(p.t, []byte(p.v))
			i++
			j++
		}
		chunks = append(chunks, c)
	}

	return chunks
}
