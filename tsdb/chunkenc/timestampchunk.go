package chunkenc

// Sketchy af.
func ConfigureSkipValueIterator(i Iterator) {
	if bi, ok := i.(*bytesIterator); ok {
		bi.skipValue = true
	}
}
