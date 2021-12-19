package ds

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

/*
	单元测试
*/

func TestSkipList_Get(t *testing.T) {
	sl := NewSkipList()
	n := sl.Get([]byte("name"))
	assert.Nil(t, nil, n)
}

func TestSkipList_Put(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("name"), "zhang san")
}

func TestSkipList_Remove(t *testing.T) {
	sl := NewSkipList()
	sl.Remove([]byte("name"))
}

/*
	场景测试
*/

// 测试基础的put get
func TestSkipList_1(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("name"), "zhang san")
	n := sl.Get([]byte("name"))

	assert.Equal(t, "zhang san", n.value)
}

// 测试相同key的覆盖功能
func TestSkipList_2(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("name"), "zhang san")
	sl.Put([]byte("name"), "li si")
	n := sl.Get([]byte("name"))

	assert.Equal(t, "li si", n.value)
}

// 测试基础的put remove
func TestSkipList_3(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("name"), "zhang san")
	sl.Remove([]byte("name"))
	n := sl.Get([]byte("name"))

	assert.Nil(t, nil, n)
}

// 测试put remove随机操作
func TestSkipList_4(t *testing.T) {
	sl := NewSkipList()

	// 存放期望结果
	mp := make(map[string]int)

	// 随机取操作
	put := 0
	remove := 1

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 5000; i++ {
		opt := rand.Intn(2)
		k := rand.Intn(100)
		v := rand.Intn(100)
		switch opt {
		case put:
			sl.Put([]byte(strconv.Itoa(k)), v)
			mp[strconv.Itoa(k)] = v
		case remove:
			sl.Remove([]byte(strconv.Itoa(k)))
			delete(mp, strconv.Itoa(k))
		}
	}

	// 校验skip list和map中的数据一致
	for k, v := range mp {
		assert.Equal(t, sl.Get([]byte(k)).value, v)
	}
}

// 获取不存在的key
func TestSkipList_5(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("a"), "1")
	sl.Put([]byte("b"), "2")
	sl.Put([]byte("d"), "4")
	n := sl.Get([]byte("c"))

	assert.Nil(t, nil, n)
}

// 打印跳表
func PrintSkipList(sl *SkipList) {
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil {
			fmt.Printf("%v->%v ", string(x.next[i].key), x.next[i].value)
			x = x.next[i]
		}
		fmt.Println()
	}
}
