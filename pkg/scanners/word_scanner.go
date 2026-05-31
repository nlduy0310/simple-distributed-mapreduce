package scanners

import (
	"bufio"
	"io"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/set"
)

var (
	delims   = wordDelimiters()
	delimSet = set.NewByteSet(delims...)
)

type WordScanner struct {
	scanner *bufio.Scanner
}

func (ws *WordScanner) Next() bool {
	return ws.scanner.Scan()
}

func (ws *WordScanner) Word() string {
	return ws.scanner.Text()
}

func (ws *WordScanner) Err() error {
	return ws.scanner.Err()
}

func NewWordScanner(reader io.Reader) WordScanner {
	scanner := bufio.NewScanner(reader)
	scanner.Split(wordSplit)
	return WordScanner{scanner: scanner}
}

func findFirstWord(data []byte) (start, end int, found bool) {
	first := -1
	for i := range len(data) {
		if !delimSet.Has(data[i]) {
			first = i
			break
		}
	}

	if first == -1 {
		return -1, -1, false
	}

	last := first
	for last+1 < len(data) && !delimSet.Has(data[last+1]) {
		last++
	}

	return first, last, true
}

func wordNotFound(atEOF bool) (int, []byte, error) {
	if atEOF {
		return 0, nil, bufio.ErrFinalToken
	}
	return 0, nil, nil
}

func wordSplit(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(data) == 0 {
		return wordNotFound(atEOF)
	}

	wstart, wend, wfound := findFirstWord(data)
	if !wfound {
		return wordNotFound(atEOF)
	}

	if wend == len(data)-1 { // maybe there's more
		if atEOF {
			return len(data), data[wstart : wend+1], bufio.ErrFinalToken
		}
		return wordNotFound(false)
	}

	return wend + 1, data[wstart : wend+1], nil
}

func wordDelimiters() []byte {
	list := make([]byte, 0)
	for i := range 256 {
		if ('a' <= i && i <= 'z') || ('A' <= i && i <= 'Z') || ('0' <= i && i <= '9') {
			continue
		}
		list = append(list, byte(i))
	}
	return list
}
