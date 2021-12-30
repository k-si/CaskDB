package ds

import (
	"bytes"
	"container/list"
)

const (
	Before = 0
	After  = 1
)

type (
	List struct {
		record LRecord
		table  LTable
	}

	LRecord map[string]*list.List
	LTable  map[string]map[string]bool
)

func NewList() *List {
	return &List{record: make(LRecord), table: make(LTable)}
}

func (l *List) Push(front bool, key string, value []byte) {
	if !l.KeyExist(key) {
		l.record[key] = list.New()
		l.table[key] = make(map[string]bool)
	}
	if front {
		l.record[key].PushFront(value)
	} else {
		l.record[key].PushBack(value)
	}
	l.table[key][string(value)] = true
}

func (l *List) Pop(front bool, key string) []byte {
	if !l.KeyExist(key) {
		return nil
	}
	var e *list.Element
	if front {
		e = l.record[key].Front()
	} else {
		e = l.record[key].Back()
	}
	v := l.record[key].Remove(e).([]byte)
	delete(l.table[key], string(v))
	return v
}

// remove some element,
// if n == 0, remove all element that meet the requirements
// if n > 0, from left to right, remove n elements that meet the requirements
// if n < 0, from right to left, remove -n elements that meet the requirements
func (l *List) Remove(key string, value []byte, n int) {
	var es []*list.Element

	if n == 0 {
		for p := l.record[key].Front(); p != nil; p = p.Next() {
			if bytes.Compare(value, p.Value.([]byte)) == 0 {
				es = append(es, p)
			}
		}
	} else if n > 0 {
		// remove -n items from left to right that equal to value
		for i, p := 0, l.record[key].Front(); i < n && p != nil; p = p.Next() {
			if bytes.Compare(value, p.Value.([]byte)) == 0 {
				es = append(es, p)
				i++
			}
		}
	} else {
		// remove n items from right to left that equal to value
		n = -n
		for i, p := 0, l.record[key].Back(); i < n && p != nil; p = p.Prev() {
			if bytes.Compare(value, p.Value.([]byte)) == 0 {
				es = append(es, p)
				i++
			}
		}
	}
	for _, item := range es {
		v := l.record[key].Remove(item).([]byte)
		delete(l.table[key], string(v))
	}
}

// find Nth element from left to right
func (l *List) Get(key string, n int) []byte {
	if !l.KeyExist(key) {
		return nil
	}
	n = l.changeIdx(key, n)
	e := l.find(key, n)
	return e.Value.([]byte)
}

func (l *List) find(key string, n int) *list.Element {
	mid := (l.record[key].Len() - 1) >> 1
	if n < mid {
		// from left to right
		i, p := 0, l.record[key].Front()
		for i < n-1 {
			p = p.Next()
			i++
		}
		return p
	} else {
		// from right to left
		i, p := 0, l.record[key].Back()
		for i < l.record[key].Len()-1-n {
			p = p.Prev()
			i++
		}
		return p
	}
}

func (l *List) Range(key string, start, stop int) (res [][]byte) {
	if !l.KeyExist(key) {
		return
	}
	start, stop, ok := correctRange(start, stop, l.record[key].Len())
	if !ok {
		return
	}
	st := l.find(key, start)
	sp := l.find(key, stop)
	for p := st; p != nil; p = p.Next() {
		res = append(res, p.Value.([]byte))
		if p == sp {
			break
		}
	}
	return
}

func (l *List) Put(key string, value []byte, n int) {
	if !l.KeyExist(key) {
		l.record[key] = list.New()
		l.table[key] = make(map[string]bool)
		l.record[key].PushBack(value)
		l.table[key][string(value)] = true
		return
	}
	n = l.changeIdx(key, n)
	e := l.find(key, n)
	e.Value = value
}

func (l *List) Insert(key string, opt, n int, value []byte) {
	if l.record[key] == nil {
		l.record[key] = list.New()
		l.table[key] = make(map[string]bool)
		l.record[key].PushBack(value)
		l.table[key][string(value)] = true
		return
	}
	n = l.changeIdx(key, n)
	e := l.find(key, n)
	if opt == Before {
		// insert before
		l.record[key].InsertBefore(value, e)
		return
	}
	if opt == After {
		// insert after
		l.record[key].InsertAfter(value, e)
	}
	return
}

func (l *List) LLen(key string) int {
	if l.KeyExist(key) {
		return l.record[key].Len()
	}
	return 0
}

func (l *List) KeyExist(key string) bool {
	_, exist := l.record[key]
	return exist
}

func (l *List) ValExist(key string, value []byte) bool {
	if !l.KeyExist(key) {
		return false
	}
	if l.table[key][string(value)] {
		return true
	}
	return false
}

// change n to valid range
func (l *List) changeIdx(key string, n int) int {
	if n >= l.record[key].Len() {
		n = l.record[key].Len() - 1
	}
	if n < 0 {
		n = 0
	}
	return n
}

func (l *List) GetAllKeys() (res []string) {
	for k, _ := range l.table {
		res = append(res, k)
	}
	return
}
