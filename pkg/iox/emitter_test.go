package iox_test

import (
	"strings"
	"testing"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/iox"
)

func TestEmitter(t *testing.T) {
	tokens := []string{"foo", "bar", "fizz", "buzz"}
	trailings := []string{"", "\n", ",", "|"}

	for _, trailing := range trailings {
		var resultWriter strings.Builder
		emitter, err := iox.NewEmitter(&resultWriter, iox.WithTrailingText(trailing))
		if err != nil {
			t.Fatalf("constructor failed: %s", err)
		}

		for _, token := range tokens {
			emitter.Emit(token)
		}

		expected := strings.Join(tokens, trailing) + trailing
		actual := resultWriter.String()
		if actual != expected {
			t.Fatalf("expected %q, got %q", expected, actual)
		}
	}
}
