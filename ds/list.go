package ds

import (
	"container/list"
)

type (
	List struct {
		record LRecord
	}

	LRecord map[string]*list.List
)

func NewList() *List {
	return &List{record: make(map[string]*list.List)}
}

func (l *List) GetRecord() map[string]*list.List {
	return l.record
}

func (l *List) Push(front bool, key string, value interface{}) {
	if l.record[key] == nil {
		l.record[key] = list.New()
	}
	if front {
		l.record[key].PushFront(value)
	} else {
		l.record[key].PushBack(value)
	}
}

func (l *List) Pop(front bool, key string) interface{} {
	if l.record[key] == nil {
		return nil
	}
	var e *list.Element
	if front {
		e = l.record[key].Front()
	} else {
		e = l.record[key].Back()
	}
	return l.record[key].Remove(e)
}