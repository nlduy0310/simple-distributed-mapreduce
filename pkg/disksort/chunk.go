package disksort

import (
	"io"
	"slices"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/iox"
)

type chunk struct {
	path  string
	data  []string
	csize uint64
}

func (c *chunk) append(text string) {
	c.data = append(c.data, text)
	c.csize += uint64(len(text))
}

func (c *chunk) size() uint64 {
	return c.csize
}

func (c *chunk) sort(compare func(string, string) int) {
	slices.SortFunc(c.data, compare)
}

func (c *chunk) flush(provideEmitter func(io.Writer) *iox.Emitter) error {
	emitter, closeEmitter, err := getEmitter(c.path, provideEmitter)
	if err != nil {
		return errx.WithContext(err, "get emitter")
	}
	defer closeEmitter()

	for _, element := range c.data {
		emitter.Emit(element)
	}
	return nil
}

func emptyChunk(path string) *chunk {
	return &chunk{
		path:  path,
		data:  make([]string, 0),
		csize: 0,
	}
}
