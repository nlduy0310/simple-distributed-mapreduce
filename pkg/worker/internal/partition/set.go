package partition

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"golang.org/x/sync/errgroup"
)

type partitioner func(word string) (idx int)

type Set struct {
	partitions  []*partition
	partitioner partitioner
	closeOnce   sync.Once
	tmpDir      string
}

func (s *Set) Record(word string, freq int) error {
	idx := s.partitioner(word)
	if err := s.partitions[idx].append(word, freq); err != nil {
		// TODO add retries?
		return errx.WithContext(err, "write to temp file")
	}

	return nil
}

func (s *Set) Finalize(parent context.Context) (_ []string, e error) {
	g, ctx := errgroup.WithContext(parent)
	dir, err := tempDir("partitions-final-*")
	if err != nil {
		return nil, errx.WithContext(err, "create temp dir")
	}

	outFiles := make([]string, 0, len(s.partitions))
	defer func() {
		if e != nil {
			for _, outFile := range outFiles {
				os.Remove(outFile)
			}
		}
	}()

	for _, partition := range s.partitions {
		p := partition
		outFile := filepath.Join(dir, p.name)
		g.Go(func() error {
			if err := p.finalize(ctx, outFile); err != nil {
				return err
			}

			outFiles = append(outFiles, outFile)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, errx.WithContext(err, "finalize partitions")
	}
	return outFiles, nil
}

func (s *Set) Close() error {
	errs := make([]error, 0)
	for _, partition := range s.partitions {
		errs = append(errs, partition.close())
	}

	if err := os.RemoveAll(s.tmpDir); err != nil {
		errs = append(errs, errx.WithContext(err, "remove temp dir"))
	}
	return errors.Join(errs...)
}

func NewSet(size int) (_ *Set, e error) {
	if size <= 0 {
		return nil, errors.New("non-positive size")
	}

	tmpPartitionsDir, err := tempDir("partitions-tmp-*")
	if err != nil {
		return nil, errx.WithContext(err, "create temp partitions dir")
	}

	partitions := make([]*partition, 0, size)
	defer func() {
		if e != nil {
			for _, p := range partitions {
				p.close()
			}
		}
	}()

	for i := range size {
		p, err := newPartition(tmpPartitionsDir, partitionName(i))
		if err != nil {
			return nil, errx.WithContext(err, fmt.Sprintf("create partition %d", i))
		}

		partitions = append(partitions, p)
	}

	return &Set{
		partitions:  partitions,
		partitioner: hashPartitioner(size),
		tmpDir:      tmpPartitionsDir,
	}, nil
}

func hashPartitioner(size int) partitioner {
	return func(word string) int {
		h := fnv.New64a()
		h.Write([]byte(word))
		return int(h.Sum64() % uint64(size))
	}
}

func partitionName(idx int) string {
	return fmt.Sprintf("partition-%d", idx)
}

func tempDir(pattern string) (string, error) {
	return os.MkdirTemp(".", pattern)
}
