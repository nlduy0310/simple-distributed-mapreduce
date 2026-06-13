package envx

import (
	"os"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/fsx"
)

type Directory struct {
	Path string
}

func DirectoryParserFunc(s string) (interface{}, error) {
	stat, err := os.Stat(s)
	if err != nil {
		return nil, err
	}

	if !stat.IsDir() {
		return nil, fsx.ErrNotADirectory
	}

	return Directory{s}, nil
}
