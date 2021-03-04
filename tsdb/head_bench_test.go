// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/conprof/db/tsdb/chunks"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/pkg/labels"
)

func BenchmarkHeadStripeSeriesCreate(b *testing.B) {
	chunkDir, err := ioutil.TempDir("", "chunk_dir")
	require.NoError(b, err)
	defer func() {
		require.NoError(b, os.RemoveAll(chunkDir))
	}()
	// Put a series, select it. GC it and then access it.
	h, err := NewHead(nil, nil, nil, 1000, chunkDir, nil, chunks.DefaultWriteBufferSize, DefaultStripeSize, nil)
	require.NoError(b, err)
	defer h.Close()

	for i := 0; i < b.N; i++ {
		_, _, _ = h.getOrCreate(uint64(i), labels.FromStrings("a", strconv.Itoa(i)))
	}
}

func BenchmarkHeadStripeSeriesCreateParallel(b *testing.B) {
	chunkDir, err := ioutil.TempDir("", "chunk_dir")
	require.NoError(b, err)
	defer func() {
		require.NoError(b, os.RemoveAll(chunkDir))
	}()
	// Put a series, select it. GC it and then access it.
	h, err := NewHead(nil, nil, nil, 1000, chunkDir, nil, chunks.DefaultWriteBufferSize, DefaultStripeSize, nil)
	require.NoError(b, err)
	defer h.Close()

	var count atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := count.Inc()
			_, _, _ = h.getOrCreate(uint64(i), labels.FromStrings("a", strconv.Itoa(int(i))))
		}
	})
}

func BenchmarkHeadAppender(b *testing.B) {
	chunkDir, err := ioutil.TempDir("", "chunk_dir")
	require.NoError(b, err)

	r := prometheus.NewRegistry()

	h, err := NewHead(r, nil, nil, 100, chunkDir, nil, chunks.DefaultWriteBufferSize, DefaultStripeSize, nil)
	require.NoError(b, err)
	defer h.Close()

	app := h.Appender(context.Background())

	v := []byte("conprof")
	ref, err := app.Add(labels.Labels{{Name: "app", Value: "conprof"}}, 0, v)
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		err := app.AddFast(ref, int64(i), v)
		require.NoError(b, err)
	}
	err = app.Commit()
	require.NoError(b, err)

	families, err := r.Gather()
	require.NoError(b, err)
	for _, family := range families {
		fmt.Printf("%+v\n", family)
	}
}
