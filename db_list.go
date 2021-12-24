package CaskDB

import (
	"CaskDB/ds"
	"CaskDB/util"
	"bytes"
	"container/list"
	"sync"
)

type ListIndex struct {
	mu  *sync.RWMutex
	idx *ds.List
}

func NewListIndex() *ListIndex {
	return &ListIndex{
		mu:  &sync.RWMutex{},
		idx: ds.NewList(),
	}
}

func (db *DB) LPush(key []byte, values ...[]byte) error {

	// check size
	if err := db.checkSize(key, nil, values...); err != nil {
		return nil
	}

	// lock
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	k := string(key)

	for _, v := range values {

		// store disk
		e := NewEntry(key, v, List, ListLPush, 0)
		if err := db.StoreFile(e); err != nil {
			return err
		}

		// store index
		f := db.activeFiles[List]
		idx := &Index{
			fileId: f.id,
			offset: f.offset - int64(e.Size()),
			value:  v,
		}
		db.listIndex.idx.Push(true, k, idx)
	}

	return nil
}

func (db *DB) RPush(key []byte, values ...[]byte) error {

	// check size
	if err := db.checkSize(key, nil, values...); err != nil {
		return nil
	}

	// lock
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	k := string(key)

	for _, v := range values {

		// store disk
		e := NewEntry(key, v, List, ListRPush, 0)
		if err := db.StoreFile(e); err != nil {
			return err
		}

		// store index
		f := db.activeFiles[List]
		idx := &Index{
			fileId: f.id,
			offset: f.offset - int64(e.Size()),
			value:  v,
		}
		db.listIndex.idx.Push(false, k, idx)
	}

	return nil
}

func (db *DB) LPop(key []byte) ([]byte, error) {

	// check size
	if err := db.checkSize(key, nil); err != nil {
		return nil, err
	}

	// lock
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	// store disk
	e := NewEntry(key, nil, List, ListLPop, 0)
	if err := db.StoreFile(e); err != nil {
		return nil, err
	}

	// pop from index
	v := db.listIndex.idx.Pop(true, string(key))
	if v == nil {
		return nil, ErrorKeyNotExist
	}

	// return the value of popped element
	val, err := db.readValue(List, v.(*Index))
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (db *DB) RPop(key []byte, values ...[]byte) ([]byte, error) {

	// check size
	if err := db.checkSize(key, nil, values...); err != nil {
		return nil, err
	}

	// lock
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	// store disk
	e := NewEntry(key, nil, List, ListRPop, 0)
	if err := db.StoreFile(e); err != nil {
		return nil, err
	}

	// pop from index
	v := db.listIndex.idx.Pop(false, string(key))
	if v == nil {
		return nil, ErrorKeyNotExist
	}

	// return the value of popped element
	idx := v.(*Index)
	val, err := db.readValue(List, idx)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// remove some element,
// if n == 0, remove all element that meet the requirements
// if n > 0, from left to right, remove n elements that meet the requirements
// if n < 0, from right to left, remove -n elements that meet the requirements
func (db *DB) LRem(key, value []byte, n int) error {

	// check size
	if err := db.checkSize(key, nil, value); err != nil {
		return nil
	}

	// lock
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	// append remove entry
	// keys = key | n
	keys := db.splice(key, util.IntToBytes(n))
	e := NewEntry(keys, value, List, ListLRem, uint32(len(key)))
	if err := db.StoreFile(e); err != nil {
		return err
	}

	// remove all items that equal to value from index
	record := db.listIndex.idx.GetRecord()
	LIdxRem(record, string(key), value, n)

	return nil
}

/*
	in order to prevent the circular reference of the package,
	some functions that related to CaskDB package are extracted,
	although it's not very elegant
*/

// remove n elements that meet the list from left to right
func LIdxRem(record ds.LRecord, key string, value []byte, n int) {
	var es []*list.Element

	if n == 0 {
		for p := record[key].Front(); p != nil; p = p.Next() {
			if bytes.Compare(value, p.Value.(*Index).Value()) == 0 {
				es = append(es, p)
			}
		}
	} else if n > 0 {
		// remove -n items from left to right that equal to value
		for i, p := 0, record[key].Front(); i < n && p != nil; p = p.Next() {
			if bytes.Compare(value, p.Value.(*Index).Value()) == 0 {
				es = append(es, p)
				i++
			}
		}
	} else {
		// remove n items from right to left that equal to value
		n = -n
		for i, p := 0, record[key].Back(); i < n && p != nil; p = p.Prev() {
			if bytes.Compare(value, p.Value.(*Index).Value()) == 0 {
				es = append(es, p)
				i++
			}
		}
	}
	for _, item := range es {
		record[key].Remove(item)
	}
}

func LIdxGet(record ds.LRecord, key string, value []byte) *Index {
	if record[key] == nil {
		return nil
	}
	for p := record[key].Front(); p != nil; p = p.Next() {
		if bytes.Compare(value, p.Value.(*Index).Value()) == 0 {
			return p.Value.(*Index)
		}
	}
	return nil
}
