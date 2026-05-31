package partition

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/disksort"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
)

type partition struct {
	name      string
	tempFile  *os.File
	finalized bool
}

func (p *partition) append(word string, freq int) error {
	if p.finalized {
		return errors.New("partition already finalized")
	}

	_, err := p.tempFile.Write([]byte(fmt.Sprintf("%s %d\n", word, freq)))
	return err
}

// close should be used for failure clean-up only, else use finalize
func (p *partition) close() error {
	return p.tempFile.Close()
}

// finalize sort and atomically rename the resulting file to outFile
func (p *partition) finalize(ctx context.Context, outFile string) error {
	if p.finalized {
		return errors.New("partition already finalized")
	}

	sorter, err := disksort.New(disksort.ProvideLineScanner, disksort.ProvideLineEmitter, compare)
	if err != nil {
		return errx.WithContext(err, "create disk sorter")
	}

	// TODO this may fail if the file is empty
	if err := sorter.Sort(ctx, p.tempFile.Name(), outFile); err != nil {
		return errx.WithContext(err, "sort file")
	}

	return nil
}

func compare(s1, s2 string) int {
	return cmp.Compare(s1, s2)
}

func newPartition(dir, name string) (*partition, error) {
	tempFile, err := os.CreateTemp(dir, name)
	if err != nil {
		return nil, errx.WithContext(err, "create temp file")
	}

	return &partition{
		name:      name,
		tempFile:  tempFile,
		finalized: false,
	}, nil
}
