package set

// ByteSet is an implementation of a set optimized for bytes.
// This struct must not be copied. It is not concurrency-safe.
type ByteSet struct {
	array [256]bool
	size  int
}

func (bs *ByteSet) Has(element byte) bool {
	return bs.array[element]
}

func (bs *ByteSet) Add(element byte) {
	if !bs.array[element] {
		bs.array[element] = true
		bs.size++
	}
}

func (bs *ByteSet) Remove(element byte) {
	if bs.array[element] {
		bs.array[element] = false
		bs.size--
	}
}

func (bs *ByteSet) Size() int {
	return bs.size
}

func NewByteSet(elements ...byte) Set[byte] {
	ret := &ByteSet{
		array: [256]bool{},
		size:  0,
	}
	for _, e := range elements {
		ret.Add(e)
	}
	return ret
}
