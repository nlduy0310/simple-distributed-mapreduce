package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/scanners"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/worker/internal/partition"
)

// TODO make this configurable
const (
	numPartitions int = 2
)

func Map(ctx context.Context, path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errx.WithContext(err, fmt.Sprintf("open %s", path))
	}
	defer file.Close()

	pSet, err := partition.NewSet(numPartitions)
	if err != nil {
		return nil, errx.WithContext(err, "create partition set")
	}
	defer pSet.Close()

	scanner := scanners.NewWordScanner(file)
	for scanner.Next() {
		word := scanner.Word()
		if err := pSet.Record(word, 1); err != nil {
			return nil, errx.WithContext(err, fmt.Sprintf("record word %q", word))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errx.WithContext(err, fmt.Sprintf("scan %s", path))
	}

	partitions, err := pSet.Finalize(ctx)
	if err != nil {
		return nil, errx.WithContext(err, "finalize partitions")
	}

	return partitions, nil
}
