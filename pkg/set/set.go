package set

type Set[T comparable] interface {
	Has(element T) bool
	Add(element T)
	Remove(element T)
	Size() int
}
