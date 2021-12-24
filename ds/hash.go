package ds

type Hash struct {
	record map[string]hkv
}

type hkv map[string]interface{}

func NewHash() *Hash {
	return &Hash{record: make(map[string]hkv)}
}

func (h *Hash) Get(key, k string) interface{} {
	if _, ok := h.record[key]; !ok {
		return nil
	}
	return h.record[key][k]
}

func (h *Hash) Put(key, k string, v interface{}) {
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
