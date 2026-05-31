package slicex

func Map[S, D any](ss []S, mapper func(S) D) []D {
	if ss == nil {
		return nil
	}

	ds := make([]D, 0, len(ss))
	for _, s := range ss {
		ds = append(ds, mapper(s))
	}
	return ds
}
