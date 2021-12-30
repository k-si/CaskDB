package ds

type Hash struct {
	record map[string]hkv
}

type hkv map[string][]byte

func NewHash() *Hash {
	return &Hash{record: make(map[string]hkv)}
}

func (h *Hash) Get(key, k string) []byte {
	if _, ok := h.record[key]; !ok {
		return nil
	}
	return h.record[key][k]
}

func (h *Hash) Put(key, k string, v []byte) {
	if _, ok := h.record[key]; !ok {
		h.record[key] = make(hkv)
	}
	h.record[key][k] = v
}

func (h *Hash) Remove(key, k string) {
	if _, ok := h.record[key]; ok {
		delete(h.record[key], k)
	}
}

func (h *Hash) GetAll(key string) (res [][]byte) {
	if !h.KeyExist(key) {
		return
	}
	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}
	return
}

func (h *Hash) KeyExist(key string) bool {
	if _, ok := h.record[key]; ok {
		return true
	}
	return false
}

func (h *Hash) FieldExist(key, k string) bool {
	if h.KeyExist(key) {
		if _, exist := h.record[key][k]; exist {
			return true
		}
	}
	return false
}

func (h *Hash) Len(key string) int {
	if h.KeyExist(key) {
		return len(h.record[key])
	}
	return 0
}