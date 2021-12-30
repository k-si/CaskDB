package ds

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestList_Push(t *testing.T) {
	lt := NewList()
	lt.Push(true, "name", []byte("zhang san"))
	lt.Push(false, "name", []byte("zhang san"))
}

func TestList_Pop(t *testing.T) {
	lt := NewList()
	lt.Pop(true, "name")
	lt.Pop(false, "name")
}

// push pop
func TestList_PushPop(t *testing.T) {
	lt := NewList()
	key := "name"

	lt.Push(true, key, []byte("zhang san"))
	lt.Push(true, key, []byte("li ming"))
	lt.Push(false, key, []byte("li si"))

	v := lt.Pop(true, "name")
	assert.Equal(t, []byte("li ming"), v)

	v = lt.Pop(false, "name")
	assert.Equal(t, []byte("li si"), v)

	v = lt.Pop(true, "gender")
	assert.Nil(t, v)
}

func TestList_KeyExist(t *testing.T) {
	lt := NewList()

	b := lt.KeyExist("name")
	assert.False(t, b)

	lt.Push(true, "name", []byte("zhang san"))
	b = lt.KeyExist("name")
	assert.True(t, b)
}

func TestList_ValExist(t *testing.T) {
	lt := NewList()

	lt.Push(true, "name", []byte("zhang san"))

	b := lt.ValExist("name", []byte("li si"))
	assert.False(t, b)

	b = lt.ValExist("name", []byte("zhang san"))
	assert.True(t, b)

	lt.Pop(true, "name")

	b = lt.ValExist("name", []byte("li si"))
	assert.False(t, b)
}

func TestList_Get(t *testing.T) {
	lt := NewList()

	v := lt.Get("name", 1)
	assert.Nil(t, v)

	lt.Push(false, "name", []byte("a"))
	lt.Push(false, "name", []byte("b"))
	lt.Push(false, "name", []byte("c"))

	v = lt.Get("name", -1)
	assert.Equal(t, "a", string(v))
	v = lt.Get("name", 0)
	assert.Equal(t, "a", string(v))
	v = lt.Get("name", 1)
	assert.Equal(t, "b", string(v))
	v = lt.Get("name", 2)
	assert.Equal(t, "c", string(v))
	v = lt.Get("name", 3)
	assert.Equal(t, "c", string(v))
}

func TestList_Insert(t *testing.T) {
	lt := NewList()
	lt.Insert("name", Before, 10, []byte("b"))
	lt.Insert("name", After, 0, []byte("c"))
	lt.Insert("name", Before, 0, []byte("a"))
	lt.Insert("name", After, 2, []byte("e"))
	lt.Insert("name", Before, 3, []byte("d"))

	res := lt.Range("name", 0, -1)
	assert.Equal(t, "a", string(res[0]))
	assert.Equal(t, "b", string(res[1]))
	assert.Equal(t, "c", string(res[2]))
	assert.Equal(t, "d", string(res[3]))
	assert.Equal(t, "e", string(res[4]))

	res = lt.Range("name", 1, -5)
	assert.Equal(t, 0, len(res))
}

func TestList_Put(t *testing.T) {
	lt := NewList()
	lt.Push(false, "name", []byte("a"))
	lt.Push(false, "name", []byte("b"))
	lt.Push(false, "name", []byte("c"))
	lt.Put("name", []byte("e"), -1)
	lt.Put("name", []byte("f"), 1)
	lt.Put("name", []byte("g"), 10)
	res := lt.Range("name", 0, -1)
	assert.Equal(t, "e", string(res[0]))
	assert.Equal(t, "f", string(res[1]))
	assert.Equal(t, "g", string(res[2]))
}
