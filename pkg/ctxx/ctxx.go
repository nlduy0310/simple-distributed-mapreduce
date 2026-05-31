package ctxx

import "context"

// Done is a quick wrapper to check if a context's done channel is closed.
// This function assumes the input is non-nil.
func Done(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
