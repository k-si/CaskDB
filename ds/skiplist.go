package ds

import (
	"math/rand"
	"time"
)

const (
	MaxLevel    int     = 32   // from 1 level to 32 level
	Probability float64 = 0.25 // promotion probability
	GE          int     = 0
	LE          int     = 1
)

type SkipList struct {
	level int // the highest level of current skip list
	size  int
	head  *skipListNode
}

type skipListNode struct {
	score  float64
	member string
	next   []*skipListNode
}

func NewSkipList() *SkipList {
	return &SkipList{
		head:  newSkipListNode(0, "", MaxLevel),
		level: 1,
		size:  0,
	}
}

func newSkipListNode(score float64, member string, level int) *skipListNode {
	return &skipListNode{
		score:  score,
		member: member,
		next:   make([]*skipListNode, level),
	}
}

// find node
// opt == 0, find first node that >= score
// opt == 1, find last node that <= score
func (sl *SkipList) Find(score float64, opt int) *skipListNode {
	x := sl.head

	if opt == GE {
		for i := sl.level - 1; i >= 0; i-- {
			for x.next[i] != nil && x.next[i].score < score {
				x = x.next[i]
			}
		}
		x = x.next[0]
		if x != nil && x.score >= score {
			return x
		}
	} else {
		for i := sl.level - 1; i >= 0; i-- {
			for x.next[i] != nil && x.next[i].score <= score {
				x = x.next[i]
			}
		}
		if x != nil && x != sl.head && x.score <= score {
			return x
		}
	}

	return nil
}

func (sl *SkipList) Insert(score float64, member string) *skipListNode {

	// store the front node of each layer into update
	update := make([]*skipListNode, MaxLevel)
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && (x.next[i].score < score || (x.next[i].score == score && (x.next[i].member < member))) {
			x = x.next[i]
		}
		update[i] = x
	}

	// insert newly node:
	// the level of newly inserted node is too high,
	// so the front node is the header, save the header to update
	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
		}
		sl.level = lvl
	}

	// other sub nodes to be inserted
	newNode := newSkipListNode(score, member, lvl)
	for i := 0; i < lvl; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}

	sl.size++

	return newNode
}

func (sl *SkipList) Delete(score float64, member string) {

	// store front node into update
	update := make([]*skipListNode, MaxLevel)
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.next[i] != nil && (x.next[i].score < score || (x.next[i].score == score && (x.next[i].member < member))) {
			x = x.next[i]
		}
		update[i] = x
	}

	// check whether the same key already exists at the position to be inserted
	x = x.next[0]
	if x != nil && x.score == score && x.member == member {

		// remove this node
		for i := 0; i < sl.level; i++ {
			if update[i].next[i] != x {
				break
			}
			update[i].next[i] = x.next[i]
		}
	}

	// chang level of skip list
	for sl.level > 1 && sl.head.next[sl.level-1] == nil {
		sl.level--
	}

	sl.size--
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
