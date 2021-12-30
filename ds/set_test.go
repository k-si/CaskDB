package ds

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSet_Add(t *testing.T) {
	set := NewSet()
	set.Add("name", "zhang san")
}

func TestSet_Remove(t *testing.T) {
	set := NewSet()
	set.Add("name", "zhang san")
	set.Remove("name", "zhang san")
	b := set.ValExist("name", "zhang san")
	assert.False(t, b)
}

func TestSet_Move(t *testing.T) {
	set := NewSet()
	set.Add("name", "zhang san")
	set.Move("name", "person", "zhang san")

	b := set.ValExist("name", "zhang san")
	assert.False(t, b)
	b = set.ValExist("person", "zhang san")
	assert.True(t, b)
}

func TestSet_Union(t *testing.T) {
	set := NewSet()
	set.Add("name", "zhang san")
	set.Add("age", "15")
	v := set.Union("name", "age")
	assert.Equal(t, 2, len(v))
	//assert.Equal(t, "zhang san", v[0])
	//assert.Equal(t, "15", v[1])
}

func TestSet_Diff(t *testing.T) {
	set := NewSet()

	set.Add("set1", "a")
	set.Add("set1", "b")

	set.Add("set2", "a")
	set.Add("set2", "b")
	set.Add("set2", "c")
	set.Add("set2", "d")

	set.Add("set3", "b")
	set.Add("set3", "e")

	v := set.Diff("set1", "set2", "set3")
	assert.Equal(t, 3, len(v))
	//assert.Equal(t, "c", v[0])
	//assert.Equal(t, "d", v[1])
	//assert.Equal(t, "e", v[2])
}

func TestSet_KeyExist(t *testing.T) {
	set := NewSet()

	b := set.KeyExist("set1")
	assert.False(t, b)

	set.Add("set1", "a")

	b = set.KeyExist("set1")
	assert.True(t, b)
}

func TestSet_ValExist(t *testing.T) {
	set := NewSet()

	b := set.ValExist("set1", "a")
	assert.False(t, b)

	set.Add("set1", "a")

	b = set.ValExist("set1", "a")
	assert.True(t, b)
}
