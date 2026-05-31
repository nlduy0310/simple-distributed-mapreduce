package set

type GenericSet[T comparable] struct {
	elements map[T]struct{}
}

func (gs *GenericSet[T]) Has(element T) bool {
	_, has := gs.elements[element]
	return has
}

func (gs *GenericSet[T]) Add(element T) {
	gs.elements[element] = struct{}{}
}

func (gs *GenericSet[T]) Remove(element T) {
	delete(gs.elements, element)
}

func (gs *GenericSet[T]) Size() int {
	return len(gs.elements)
}

func NewGenericSet[T comparable](elements ...T) Set[T] {
	ret := &GenericSet[T]{elements: make(map[T]struct{})}
	for _, e := range elements {
		ret.Add(e)
	}
	return ret
}
