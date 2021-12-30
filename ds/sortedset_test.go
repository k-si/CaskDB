package ds

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSortedSet_Add(t *testing.T) {
	ss := NewSortedSet()
	ss.Add("stu", "a", 90)
	ss.Add("stu", "a", 90)
	ss.Add("stu", "b", 85)
	ss.Add("stu", "b", 89)
	ss.Add("stu", "c", 90)
	res := ss.Top("stu", 5)

	m, s := res[0].(string), res[1].(float64)
	assert.Equal(t, "b", m)
	assert.Equal(t, float64(89), s)

	m, s = res[2].(string), res[3].(float64)
	assert.Equal(t, "a", m)
	assert.Equal(t, float64(90), s)

	m, s = res[4].(string), res[5].(float64)
	assert.Equal(t, "c", m)
	assert.Equal(t, float64(90), s)
}

func TestSortedSet_Remove(t *testing.T) {
	ss := NewSortedSet()
	ss.Add("stu", "a", 90)
	ss.Remove("tea", "a")
	ss.Remove("stu", "a")
	_, score := ss.GetScore("stu", "a")
	assert.Equal(t, float64(0), score)
}

func TestSortedSet_RangeByScore(t *testing.T) {
	ss := NewSortedSet()
	ss.Add("stu", "a", 90)
	ss.Add("stu", "b", 85)
	ss.Add("stu", "c", 100)
	ss.Add("stu", "d", 60)

	res := ss.RangeByScore("stu", 60, 100)
	assert.Equal(t, "d", res[0].(string))
	assert.Equal(t, float64(60), res[1].(float64))
	assert.Equal(t, "b", res[2].(string))
	assert.Equal(t, float64(85), res[3].(float64))
	assert.Equal(t, "a", res[4].(string))
	assert.Equal(t, float64(90), res[5].(float64))
	assert.Equal(t, "c", res[6].(string))
	assert.Equal(t, float64(100), res[7].(float64))

	res = ss.RangeByScore("stu", 50, 55)
	assert.Equal(t, 0, len(res))

	res = ss.RangeByScore("stu", 110, 120)
	assert.Equal(t, 0, len(res))
}

func TestSortedSet_GetCard(t *testing.T) {
	ss := NewSortedSet()
	ss.Add("stu", "a", 90)
	ss.Add("stu", "a", 90)
	ss.Add("stu", "b", 85)
	ss.Add("stu", "b", 89)
	ss.Add("stu", "c", 90)
	c := ss.GetCard("stu")
	assert.Equal(t, 3, c)
}

func TestSortedSet_GetScore(t *testing.T) {

}

func TestSortedSet_MemberExist(t *testing.T) {
	ss := NewSortedSet()
	ss.Add("stu", "a", 90)
	b := ss.MemberExist("stu", "a")
	assert.True(t, b)

	ss.Remove("stu", "a")
	b = ss.MemberExist("stu", "a")
	assert.False(t, b)
}
