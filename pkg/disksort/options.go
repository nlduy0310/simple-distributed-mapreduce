package disksort

type config struct {
	chunkSize     uint64
	sortBatchSize int
}

type Option func(*config)

// WithChunkSize returns the option to set the approximate chunk size in bytes.
func WithChunkSize(size uint64) Option {
	return func(c *config) {
		c.chunkSize = size
	}
}

// WithSortBatchSize returns the option to set the number of chunks merge/sort at each iteration
func WithSortBatchSize(size int) Option {
	return func(c *config) {
		c.sortBatchSize = size
	}
}
