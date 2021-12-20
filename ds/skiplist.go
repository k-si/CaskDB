package ds

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

const (
	MaxLevel    int     = 32 // from 1 level to 32 level
	Probability float64 = 1 / math.E
)

type SkipList struct {
	header *skipListNode
	level  int         // the highest level of current skip list
	size   int
}

type skipListNode struct {
	key   []byte
	value interface{}
	next  []*skipListNode
}

func NewSkipList() *SkipList {
	return &SkipList{
		header: newSkipListNode(nil, nil, MaxLevel),
		level:  1,
		size:   0,
	}
}

func newSkipListNode(key []byte, value interface{}, level int) *skipListNode {
	return &skipListNode{
		key:   key,
		value: value,
		next:  make([]*skipListNode, level),
	}
}

func (skn *skipListNode) Value() interface{} {
	return skn.value
}

func (sl *SkipList) Get(key []byte) interface{} {
	if n := sl.find(key); n != nil {
		return n.value
	}
	return nil
}

// find node
func (sl *SkipList) find(key []byte) *skipListNode {
	x := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && bytes.Compare(x.key, key) == 0 {
		return x
	}

	return nil
}

func (sl *SkipList) Put(key []byte, value interface{}) {

	// store the front node of each layer into update
	update := make([]*skipListNode, MaxLevel)
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}

	// check whether the same key already exists at the position to be inserted
	x = x.next[0]
	if x != nil && bytes.Compare(x.key, key) == 0 {
		x.value = value
		return
	}

	// insert newly node:
	// the level of newly inserted node is too high,
	// so the front node is the header, save the header to update
	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.header
		}
		sl.level = lvl
	}

	// other sub nodes to be inserted
	newNode := newSkipListNode(key, value, lvl)
	for i := 0; i < lvl; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.size++
}

func (sl *SkipList) Remove(key []byte) {

	// store front node into update
	update := make([]*skipListNode, MaxLevel)
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].key, key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}

	// check whether the same key already exists at the position to be inserted
	x = x.next[0]
	if x != nil && bytes.Compare(x.key, key) == 0 {

		// remove this node
		for i := 0; i < sl.level; i++ {
			if update[i].next[i] != x {
				break
			}
			update[i].next[i] = x.next[i]
		}
	}
}

// get random level
func (sl *SkipList) randomLevel() int {
	level := 1

	// for each cycle, the probability is multiplied by 'Probability'
	for level < MaxLevel && random() < Probability {
		level++
	}
	//fmt.Println(level)

	return level
}

// generate random numbers from 0 to 1
func random() float64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Float64()
}
