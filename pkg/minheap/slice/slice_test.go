package slice_test

import (
	"slices"
	"testing"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/minheap/slice"
)

func TestSlicePushPop(t *testing.T) {
	lessFunc := func(a, b int) bool {
		return a < b
	}
	mh := slice.New(lessFunc, 0)

	inserts := []int{5, 3, 4, 1, 2}
	for _, v := range inserts {
		mh.Push(v)
	}

	expected := slices.SortedFunc(slices.Values(inserts), func(a, b int) int {
		return a - b
	})

	actual := make([]int, 0, len(expected))
	for range len(expected) {
		v, ok := mh.Pop()
		if !ok {
			t.Fatalf("expected %d successful pops, got %d", len(expected), len(actual))
		}

		actual = append(actual, v)
	}

	for i := range len(expected) {
		if expectedVal, actualVal := expected[i], actual[i]; expectedVal != actualVal {
			t.Fatalf("expected pop #%d to be %d, got %d", i+1, expectedVal, actualVal)
		}
	}
}
