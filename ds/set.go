package ds

type (
	Set struct {
		record SRecord
	}

	SRecord map[string]map[string]struct{}
)

func NewSet() *Set {
	return &Set{record: make(SRecord)}
}

func (s *Set) Add(key string, value string) {
	if s.record[key] == nil {
		s.record[key] = make(map[string]struct{})
	}
	s.record[key][value] = struct{}{}
}

func (s *Set) Remove(key, value string) {
	if _, ok := s.record[key]; !ok {
		return
	}
	delete(s.record[key], value)
}

// move value from src set to dest set
func (s *Set) Move(src, dest string, value string) {
	if _, ok := s.record[src]; !ok {
		return
	}
	if _, ok := s.record[src][value]; !ok {
		return
	}

	if s.record[dest] == nil {
		s.record[dest] = make(map[string]struct{})
	}
	s.record[dest][value] = struct{}{}
	delete(s.record[src], value)
}

// set1 + set2 + ... + setN
func (s *Set) Union(keys ...string) []string {
	m := make(map[string]struct{})

	for _, k := range keys {
		if s.record[k] != nil {
			for v := range s.record[k] {
				m[v] = struct{}{}
			}
		}
	}

	res, i := make([]string, len(m)), 0

	for k := range m {
		res[i] = k
		i++
	}

	return res
}

func (s *Set) Diff(keys ...string) []string {
	if len(keys) == 0 {
		return nil
	}
	if _, ok := s.record[keys[0]]; !ok {
		return nil
	}

	m := make(map[string]struct{})

	for i := 1; i < len(keys); i++ {
		for k := range s.record[keys[i]] {
			if !s.ValExist(keys[0], k) {
				m[k] = struct{}{}
			}
		}
	}

	res, i := make([]string, len(m)), 0
	for k := range m {
		res[i] = k
		i++
	}
	return res
}

func (s *Set) Scan(key string) (res [][]byte) {
	if !s.KeyExist(key) {
		return
	}
	for v, _ := range s.record[key] {
		res = append(res, []byte(v))
	}
	return res
}

func (s *Set) KeyExist(key string) bool {
	if _, ok := s.record[key]; ok {
		return true
	}
	return false
}

func (s *Set) ValExist(key, value string) bool {
	if s.KeyExist(key) {
		if _, ok := s.record[key][value]; ok {
			return true
		}
	}
	return false
}

func (s *Set) Len(key string) int {
	if s.KeyExist(key) {
		return len(s.record[key])
	}
	return 0
}
