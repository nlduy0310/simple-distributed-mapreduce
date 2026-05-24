package ctxx

import "context"

func Expired(ctx context.Context) bool {
	return ctx.Err() != nil
}
