package ds

type Set struct {
	record map[string]map[string]struct{}
}

func NewSet() *Set {
	return &Set{record: make(map[string]map[string]struct{})}
}
