package ds

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHash(t *testing.T) {
	h := NewHash()

	v := h.Get("person", "zhang san")
	assert.Nil(t, v)

	h.Remove("people", "zhang san")
	h.Remove("person", "zhang san")

	h.Put("person", "zhang san", []byte("18"))
	v = h.Get("person", "zhang san")
	assert.Equal(t, []byte("18"), v)

	h.Put("person", "zhang san", []byte("19"))
	v = h.Get("person", "zhang san")
	assert.Equal(t, []byte("19"), v)

	h.Put("person", "li si", []byte("19"))
	v = h.Get("person", "zhang san")
	assert.Equal(t, []byte("19"), v)
	v = h.Get("person", "li si")
	assert.Equal(t, []byte("19"), v)

	h.Remove("person", "li si")
	v = h.Get("person", "li si")
	assert.Nil(t, v)
}
