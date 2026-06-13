package zero

func Of[T any]() T {
	var zero T
	return zero
}
