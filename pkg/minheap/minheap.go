package minheap

type MinHeap[T any] interface {
	Push(element T)
	Pop() (T, bool)
	Peek() (T, bool)
	Size() int
}
