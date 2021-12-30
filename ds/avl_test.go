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
	avl.Put([]byte("0"), "a")
	avl.Put([]byte("1"), "b")
	avl.Put([]byte("2"), "c")
	avl.Put([]byte("3"), "d")
	avl.Put([]byte("4"), "e")
	avl.Put([]byte("5"), "f")
	PrintAVLTree(avl.root)
}

func TestAVLTree_Remove_1(t *testing.T) {
	avl := NewAVLTree()
	avl.Put([]byte("1"), 1)
	avl.Put([]byte("2"), 2)
	avl.Put([]byte("3"), 3)
	PrintAVLTree(avl.root)
	avl.Remove([]byte("2"))
	PrintAVLTree(avl.root)
}

// test random option
func TestAVLTree(t *testing.T) {
	avl := NewAVLTree()

	// store expect value
	mp := make(map[string]int)

	// random option
	put := 0
	rem := 1

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < 10000; i++ {
		opt := rand.Intn(2)
		k := rand.Intn(1000)
		v := rand.Intn(1000)
		switch opt {
		case put:
			avl.Put([]byte(strconv.Itoa(k)), v)
			mp[strconv.Itoa(k)] = v
		case rem:
			avl.Remove([]byte(strconv.Itoa(k)))
			delete(mp, strconv.Itoa(k))
		}
	}
	//PrintAVLTree(avl.root)

	// check actual value
	for k, v := range mp {
		assert.Equal(t, avl.Get([]byte(k)), v)
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
//BenchmarkAVLTree_Get-8          18951756                63.90 ns/op            7 B/op          0 allocs/op
//PASS
//ok      CaskDB/ds       2.309s

func BenchmarkAVLTree_Get(b *testing.B) {
	avl := PrepareAVL()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		avl.Get([]byte(strconv.Itoa(i)))
	}
}

//goos: darwin
//goarch: arm64
//pkg: CaskDB/ds
//BenchmarkAVLTree_Put-8             10000            156399 ns/op              87 B/op          3 allocs/op
//PASS
//ok      CaskDB/ds       1.846s

func BenchmarkAVLTree_Put(b *testing.B) {
	avl := PrepareAVL()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		avl.Put([]byte(strconv.Itoa(i)), i)
	}
}

//goos: darwin
//goarch: arm64
//pkg: CaskDB/ds
//BenchmarkAVLTree_Remove-8       52641296                24.01 ns/op            7 B/op          0 allocs/op
//PASS
//ok      CaskDB/ds       1.689s

func BenchmarkAVLTree_Remove(b *testing.B) {
	avl := PrepareAVL()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		avl.Remove([]byte(strconv.Itoa(i)))
	}
}
