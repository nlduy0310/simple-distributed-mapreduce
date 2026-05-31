package slice

import "github.com/nlduy0310/simple-distributed-mapreduce/pkg/minheap"

type sliceHeap[T any] struct {
	elements []T
	lessFunc func(T, T) bool
}

func (sh *sliceHeap[T]) Push(element T) {
	sh.elements = append(sh.elements, element)
	siftUp(sh.elements, len(sh.elements)-1, sh.lessFunc)
}

func (sh *sliceHeap[T]) Pop() (T, bool) {
	n := len(sh.elements)
	if n == 0 {
		var t0 T
		return t0, false
	}

	ret := sh.elements[0]
	sh.elements[0], sh.elements[n-1] = sh.elements[n-1], sh.elements[0]
	sh.elements = sh.elements[:n-1]
	if n > 1 {
		siftDown(sh.elements, 0, sh.lessFunc)
	}

	return ret, true
}

func (sh *sliceHeap[T]) Peek() (T, bool) {
	if len(sh.elements) == 0 {
		var t0 T
		return t0, false
	}

	return sh.elements[0], true
}

func (sh *sliceHeap[T]) Size() int {
	return len(sh.elements)
}

// New returns a new slice-based minheap.
// `preAllocate` is used to set the capacity for the underlying slice,
// if `preAllocate` <= 0, use the default slice capacity.
func New[T any](lessFunc func(T, T) bool, preAllocate int) minheap.MinHeap[T] {
	var elements []T
	if preAllocate > 0 {
		elements = make([]T, 0, preAllocate)
	} else {
		elements = make([]T, 0)
	}

	return &sliceHeap[T]{
		elements: elements,
		lessFunc: lessFunc,
	}
}

func siftUp[T any](elements []T, at int, lessThan func(T, T) bool) {
	parent := parentIdx(at)
	if parent < 0 {
		return
	}

	if lessThan(elements[at], elements[parent]) {
		elements[at], elements[parent] = elements[parent], elements[at]
		siftUp(elements, parent, lessThan)
	}
}

func siftDown[T any](elements []T, at int, lessThan func(T, T) bool) {
	minIdx, minVal := at, elements[at]

	if left := leftChildIdx(at); left >= 0 && left < len(elements) && lessThan(elements[left], minVal) {
		minIdx, minVal = left, elements[left]
	}

	if right := rightChildIdx(at); right >= 0 && right < len(elements) && lessThan(elements[right], minVal) {
		minIdx, minVal = right, elements[right]
	}

	if minIdx == at {
		return
	}

	elements[at], elements[minIdx] = elements[minIdx], elements[at]
	siftDown(elements, minIdx, lessThan)
}

func parentIdx(childIdx int) int {
	return (childIdx - 1) / 2
}

func leftChildIdx(parentIdx int) int {
	return 2*parentIdx + 1
}

func rightChildIdx(parentIdx int) int {
	return 2*parentIdx + 2
}
