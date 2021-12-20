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
	functional test
*/

func TestAVLTree_Get(t *testing.T) {
	avl := NewAVLTree()
	n := avl.Get([]byte("name"))
	assert.Nil(t, nil, n)
}

func TestAVLTree_Put(t *testing.T) {
	avl := NewAVLTree()
	avl.Put([]byte("name"), "zhang san")
}

func TestAVLTree_Remove(t *testing.T) {
	avl := NewAVLTree()
	avl.Remove([]byte("name"))
}

func TestAVLTree_1(t *testing.T) {
	avl := NewAVLTree()

	avl.Put([]byte("name"), "zhang san")
	v := avl.Get([]byte("name"))

	assert.Equal(t, "zhang san", v)
}

func TestAVLTree_2(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("name"), "zhang san")
	sl.Put([]byte("name"), "li si")
	v := sl.Get([]byte("name"))

	assert.Equal(t, "li si", v)
}

func TestAVLTree_3(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("name"), "zhang san")
	sl.Remove([]byte("name"))
	v := sl.Get([]byte("name"))

	assert.Nil(t, nil, v)
}

// exception test
func TestAVLTree_4(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("a"), "1")
	sl.Put([]byte("b"), "2")
	sl.Put([]byte("d"), "4")
	v := sl.Get([]byte("c"))

	assert.Nil(t, nil, v)
}

// test random option
func TestAVLTree_5(t *testing.T) {
	sl := NewSkipList()

	// store expect value
	mp := make(map[string]int)

	// random option
	put := 0
	rem := 1

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 5000; i++ {
		opt := rand.Intn(2)
		k := rand.Intn(100)
		v := rand.Intn(100)
		switch opt {
		case put:
			sl.Put([]byte(strconv.Itoa(k)), v)
			mp[strconv.Itoa(k)] = v
		case rem:
			sl.Remove([]byte(strconv.Itoa(k)))
			delete(mp, strconv.Itoa(k))
		}
	}

	// check actual value
	for k, v := range mp {
		assert.Equal(t, sl.Get([]byte(k)), v)
	}
}

func TestAVLTreeUse(t *testing.T) {
	avl := NewAVLTree()
	avl.Put([]byte("1"), "a")
	avl.Put([]byte("2"), "b")
	avl.Put([]byte("3"), "c")
	avl.Put([]byte("4"), "d")
	avl.Put([]byte("5"), "e")
	PrintAVLTree(avl.root)
}

func PrintAVLTree(root *aVLTreeNode) {
	if root != nil {
		fmt.Printf("%v-%v", string(root.key), root.value)
		if root.left != nil || root.right != nil {
			fmt.Printf("(")
			if root.left != nil {
				PrintAVLTree(root.left)
			}
			fmt.Printf(", ")
			if root.right != nil {
				PrintAVLTree(root.right)
			}
			fmt.Printf(")")
		}
	}
}

/*
	benchmark test
*/

func PrepareAVL() *AVLTree {
	avl := NewAVLTree()

	put := 0
	rem := 1

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 50000; i++ {
		opt := rand.Intn(2)
		k := rand.Intn(100)
		v := rand.Intn(100)
		switch opt {
		case put:
			avl.Put([]byte(strconv.Itoa(k)), v)
		case rem:
			avl.Remove([]byte(strconv.Itoa(k)))
		}
	}

	return avl
}

//goos: darwin
//goarch: arm64
//pkg: CaskDB/ds
//BenchmarkAVLTree_Get-8          16574833                63.59 ns/op            7 B/op          0 allocs/op
//PASS
//ok      CaskDB/ds       1.335s

// in my test, the fastest is 58s and the slowest is 68s, this is unstable
func BenchmarkAVLTree_Get(b *testing.B) {
	sl := PrepareSL()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Get([]byte(strconv.Itoa(i)))
	}
}

//goos: darwin
//goarch: arm64
//pkg: CaskDB/ds
//BenchmarkAVLTree_Put-8             68772             17792 ns/op             101 B/op          4 allocs/op
//PASS
//ok      CaskDB/ds       1.580s

func BenchmarkAVLTree_Put(b *testing.B) {
	sl := PrepareSL()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Put([]byte(strconv.Itoa(i)), i)
	}
}

//goos: darwin
//goarch: arm64
//pkg: CaskDB/ds
//BenchmarkAVLTree_Remove-8       36373234                31.30 ns/op            7 B/op          0 allocs/op
//PASS
//ok      CaskDB/ds       1.402s

func BenchmarkAVLTree_Remove(b *testing.B) {
	sl := PrepareSL()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Remove([]byte(strconv.Itoa(i)))
	}
}
