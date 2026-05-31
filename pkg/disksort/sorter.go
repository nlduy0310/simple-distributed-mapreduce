package disksort

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/ctxx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/iox"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/minheap/slice"
)

const (
	checkCtxEvery uint64 = 10000
)

type Sorter struct {
	cfg             config
	scannerProvider func(io.Reader) *bufio.Scanner
	emitterProvider func(io.Writer) *iox.Emitter
	compare         func(string, string) int
}

func (s *Sorter) Sort(ctx context.Context, inFile, outFile string) error {
	chunkPaths, chunksDir, err := s.chunkAndSort(ctx, inFile)
	if err != nil {
		return errx.WithContext(err, "chunk input file")
	}
	defer os.RemoveAll(chunksDir)

	sortedChunkPath, err := s.mergeSortedChunks(ctx, chunkPaths)
	if err != nil {
		return errx.WithContext(err, "sort chunks")
	}

	// TODO handle 'invalid cross-device link' case
	err = os.Rename(sortedChunkPath, outFile)
	if err != nil {
		defer os.Remove(sortedChunkPath)
		return errx.WithContext(err, "rename sorted chunk")
	}

	return nil
}

func (s *Sorter) mergeSortedChunks(ctx context.Context, chunkPaths []string) (string, error) {
	if len(chunkPaths) == 0 {
		return "", errors.New("no input")
	}

	tmpDir, err := os.MkdirTemp(filepath.Dir(chunkPaths[0]), "merges-*")
	if err != nil {
		return "", errx.WithContext(err, "create temp dir")
	}
	defer os.RemoveAll(tmpDir)

	currentChunks := chunkPaths
	groupCount := 0
	batchSize := s.cfg.sortBatchSize
	for len(currentChunks) > 1 {
		if ctxx.Done(ctx) {
			return "", ctx.Err()
		}

		numGroups := (len(currentChunks) / batchSize) + 1
		nextChunks := make([]string, 0, numGroups)
		for g := range numGroups {
			path := filepath.Join(tmpDir, fmt.Sprintf("merge-group-%d", groupCount))
			from := g * batchSize
			to := min((g+1)*batchSize-1, len(currentChunks)-1)
			if err := mergeSortedChunks(ctx, currentChunks[from:to+1], path, s.scannerProvider, s.emitterProvider, s.compare); err != nil {
				return "", errx.WithContext(err, fmt.Sprintf("merge group %d", groupCount))
			}

			groupCount++
			nextChunks = append(nextChunks, path)
		}

		currentChunks = nextChunks
	}

	return currentChunks[0], nil
}

func mergeSortedChunks(ctx context.Context, chunkPaths []string, outPath string, provideScanner func(io.Reader) *bufio.Scanner, provideEmitter func(io.Writer) *iox.Emitter, compare func(string, string) int) error {
	emitter, closeEmitter, err := getEmitter(outPath, provideEmitter)
	if err != nil {
		return errx.WithContext(err, fmt.Sprintf("get emitter to %s", outPath))
	}
	defer closeEmitter()

	scanners, closeScanners, err := getScanners(chunkPaths, provideScanner)
	if err != nil {
		return errx.WithContext(err, "get scanners")
	}
	defer closeScanners()

	// best effort
	if ctxx.Done(ctx) {
		return ctx.Err()
	}

	type entry struct {
		value    string
		chunkIdx int
	}
	entryLessFunc := func(e1, e2 entry) bool {
		return compare(e1.value, e2.value) < 0
	}
	heap := slice.New(entryLessFunc, len(chunkPaths))

	// initialize
	for i, scanner := range scanners {
		v, ok, err := nextValue(scanner)
		if err != nil {
			return errx.WithContext(err, fmt.Sprintf("scan from %s", chunkPaths[i]))
		} else if !ok {
			continue
		}

		heap.Push(entry{value: v, chunkIdx: i})
	}

	// merge until heap is empty
	var valuesProcessed uint64 = 0
	for ; heap.Size() > 0; valuesProcessed++ {
		if valuesProcessed%checkCtxEvery == 0 && ctxx.Done(ctx) {
			return ctx.Err()
		}

		e, _ := heap.Pop()
		if err := emitter.Emit(e.value); err != nil {
			return errx.WithContext(err, fmt.Sprintf("emit to %s", outPath))
		}

		v, ok, err := nextValue(scanners[e.chunkIdx])
		if err != nil {
			return errx.WithContext(err, fmt.Sprintf("scan from %s", chunkPaths[e.chunkIdx]))
		} else if !ok {
			continue
		}
		heap.Push(entry{value: v, chunkIdx: e.chunkIdx})
	}

	return nil
}

func getEmitter(file string, provide func(io.Writer) *iox.Emitter) (*iox.Emitter, func() error, error) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return nil, nil, errx.WithContext(err, fmt.Sprintf("open %s", file))
	}
	close := func() error {
		return f.Close()
	}

	emitter := provide(f)
	if emitter == nil {
		close()
		return nil, nil, errors.New("received nil emitter")
	}

	return emitter, close, nil
}

func getScanners(files []string, provide func(io.Reader) *bufio.Scanner) ([]*bufio.Scanner, func() error, error) {
	scanners := make([]*bufio.Scanner, 0, len(files))
	closers := make([]func() error, 0, len(files))
	closeAll := func() error {
		errs := make([]error, 0)
		for _, close := range closers {
			errs = append(errs, close())
		}
		return errors.Join(errs...)
	}

	for _, file := range files {
		scanner, close, err := getScanner(file, provide)
		if err != nil {
			closeAll()
			return nil, nil, errx.WithContext(err, fmt.Sprintf("get scanner from %s", file))
		}
		scanners = append(scanners, scanner)
		closers = append(closers, close)
	}

	return scanners, closeAll, nil
}

func getScanner(file string, provide func(io.Reader) *bufio.Scanner) (*bufio.Scanner, func() error, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, nil, errx.WithContext(err, fmt.Sprintf("open %s", file))
	}
	close := func() error {
		return f.Close()
	}

	scanner := provide(f)
	if scanner == nil {
		close()
		return nil, nil, errors.New("received nil scanner")
	}

	return scanner, close, nil
}

func nextValue(scanner *bufio.Scanner) (string, bool, error) {
	hasNext := scanner.Scan()
	if !hasNext {
		return "", false, scanner.Err()
	}

	return scanner.Text(), true, nil
}

func (s *Sorter) chunkAndSort(ctx context.Context, file string) (chunkPaths []string, chunksDir string, e error) {
	scanner, closeScanner, err := getScanner(file, s.scannerProvider)
	if err != nil {
		return nil, "", errx.WithContext(err, fmt.Sprintf("get scanner from %s", file))
	}
	defer closeScanner()

	tmpDir, err := os.MkdirTemp(filepath.Dir(file), "chunks-*")
	if err != nil {
		return nil, "", errx.WithContext(err, "create temp dir")
	}
	defer func() {
		if e != nil {
			os.RemoveAll(tmpDir)
		}
	}()

	ret := make([]string, 0)
	chunkNum := 0
	for ; ; chunkNum++ {
		if ctxx.Done(ctx) {
			return nil, "", ctx.Err()
		}

		path := filepath.Join(tmpDir, fmt.Sprintf("chunk-%d", chunkNum))
		c := emptyChunk(path)
		if err := fillChunk(c, scanner, s.cfg.chunkSize); err != nil {
			return nil, "", errx.WithContext(err, fmt.Sprintf("fill chunk %s", path))
		} else if c.size() == 0 {
			break
		}

		c.sort(s.compare)

		if err := c.flush(s.emitterProvider); err != nil {
			return nil, "", errx.WithContext(err, fmt.Sprintf("flush chunk %s", path))
		}

		ret = append(ret, path)
	}

	return ret, tmpDir, nil
}

func fillChunk(c *chunk, scanner *bufio.Scanner, approxSize uint64) error {
	for c.csize < approxSize && scanner.Scan() {
		t := scanner.Text()
		c.append(t)
	}

	return scanner.Err()
}
