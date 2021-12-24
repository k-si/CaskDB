package ds

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestList_Push(t *testing.T) {
	lt := NewList()
	lt.Push(true, "name", "zhang san")
	lt.Push(false, "name", "zhang san")
}

func TestList_Pop(t *testing.T) {
	lt := NewList()
	lt.Pop(true, "name")
	lt.Pop(false, "name")
}

// push pop
func TestList_1(t *testing.T) {
	lt := NewList()
	key := "name"

	lt.Push(true, key, "zhang san")
	lt.Push(true, key, "li ming")
	lt.Push(false, key, "li si")

	v := lt.Pop(true, "name")
	assert.Equal(t, "li ming", v)

	v = lt.Pop(false, "name")
	assert.Equal(t, "li si", v)

	v = lt.Pop(true, "gender")
	assert.Nil(t, v)
}
