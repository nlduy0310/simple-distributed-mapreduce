package disksort

import (
	"bufio"
	"io"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/iox"
)

func ProvideLineScanner(reader io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)
	return scanner
}

func ProvideLineEmitter(writer io.Writer) *iox.Emitter {
	emitter, _ := iox.NewEmitter(writer, iox.WithTrailingText("\n"))
	return emitter
}
