package CaskDB

import (
	"CaskDB/ds"
	"CaskDB/util"
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
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValsSize(values...); err != nil {
		return err
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
		db.listIndex.idx.Push(true, k, v)
	}

	return nil
}

func (db *DB) LPop(key []byte) ([]byte, error) {

	// check size
	if err := db.checkKeySize(key); err != nil {
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

	return v, nil
}

func (db *DB) RPush(key []byte, values ...[]byte) error {

	// check size
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValsSize(values...); err != nil {
		return err
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
		db.listIndex.idx.Push(false, k, v)
	}

	return nil
}

func (db *DB) RPop(key []byte) ([]byte, error) {

	// check size
	if err := db.checkKeySize(key); err != nil {
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

	return v, nil
}

// remove some element,
// if n == 0, remove all element that meet the requirements
// if n > 0, from left to right, remove n elements that meet the requirements
// if n < 0, from right to left, remove -n elements that meet the requirements
func (db *DB) LRem(key, value []byte, n int) error {

	// check size
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValSize(value); err != nil {
		return err
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
	db.listIndex.idx.Remove(string(key), value, n)

	return nil
}

// get Nth element
func (db *DB) LIndex(key []byte, n int) ([]byte, error) {

	// check
	if err := db.checkKeySize(key); err != nil {
		return nil, err
	}

	// lock
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	v := db.listIndex.idx.Get(string(key), n)
	return v, nil
}

func (db *DB) LInsert(key, value []byte, n int) error {
	// check size
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValSize(value); err != nil {
		return err
	}

	// lock
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	// store disk
	keys := db.splice(key, util.IntToBytes(n))
	e := NewEntry(keys, value, List, ListLInsert, uint32(len(key)))
	if err := db.StoreFile(e); err != nil {
		return err
	}

	db.listIndex.idx.Insert(string(key), 0, n, value)
	return nil
}

func (db *DB) LRInsert(key, value []byte, n int) error {
	// check size
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValSize(value); err != nil {
		return err
	}

	// lock
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	// store disk
	keys := db.splice(key, util.IntToBytes(n))
	e := NewEntry(keys, value, List, ListRInsert, uint32(len(key)))
	if err := db.StoreFile(e); err != nil {
		return err
	}

	db.listIndex.idx.Insert(string(key), 1, n, value)
	return nil
}

// cover the value of Nth element
func (db *DB) LSet(key, value []byte, n int) error {

	// check
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValSize(value); err != nil {
		return err
	}

	// lock
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	// store disk
	keys := db.splice(key, util.IntToBytes(n))
	e := NewEntry(keys, value, List, ListLSet, uint32(len(key)))
	if err := db.StoreFile(e); err != nil {
		return err
	}

	db.listIndex.idx.Put(string(key), value, n)
	return nil
}

func (db *DB) LRange(key []byte, start, stop int) ([][]byte, error) {

	// check size
	if err := db.checkKeySize(key); err != nil {
		return nil, err
	}

	// lock
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	res := db.listIndex.idx.Range(string(key), start, stop)
	return res, nil
}

func (db *DB) LExist(key, value []byte) bool {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	return db.listIndex.idx.ValExist(string(key), value)
}

func (db *DB) LLen(key []byte) int {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	return db.listIndex.idx.LLen(string(key))
}
