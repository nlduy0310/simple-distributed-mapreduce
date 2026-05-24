package errx

import (
	"context"
	"errors"
	"fmt"
)

func OneOf(err error, errs ...error) bool {
	for _, target := range errs {
		if errors.Is(err, target) {
			return true
		}
	}

	return false
}

func WithContext(err error, ctx string) error {
	return fmt.Errorf("%s: %w", ctx, err)
}

func WithContextErr(err error, ctx error) error {
	return fmt.Errorf("%w: %w", ctx, err)
}

func IsContext(err error) bool {
	return OneOf(err, context.Canceled, context.DeadlineExceeded)
}
