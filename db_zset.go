package CaskDB

import (
	"github.com/k-si/CaskDB/ds"
	"github.com/k-si/CaskDB/util"
	"sync"
)

type ZSetIndex struct {
	mu  *sync.RWMutex
	idx *ds.SortedSet
}

func NewZSetIndex() *ZSetIndex {
	return &ZSetIndex{
		mu:  &sync.RWMutex{},
		idx: ds.NewSortedSet(),
	}
}

func (db *DB) ZAdd(key []byte, score float64, member []byte) error {

	// check size
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValSize(member); err != nil {
		return err
	}

	// lock
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	// store disk
	keys := db.splice(key, util.Float64ToBytes(score))
	e := NewEntry(keys, member, ZSet, ZSetZAdd, uint32(len(key)))
	if err := db.StoreFile(e); err != nil {
		return err
	}

	// store index
	db.zsetIndex.idx.Add(string(key), string(member), score)
	return nil
}

func (db *DB) ZRem(key, member []byte) error {

	// check size
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValSize(member); err != nil {
		return err
	}

	// lock
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	// store disk
	e := NewEntry(key, member, ZSet, ZSetZRem, 0)
	if err := db.StoreFile(e); err != nil {
		return err
	}
	db.zsetIndex.idx.Remove(string(key), string(member))

	return nil
}

func (db *DB) ZScoreRange(key []byte, from, to float64) ([]interface{}, error) {

	// check
	if err := db.checkKeySize(key); err != nil {
		return nil, err
	}

	// lock
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	res := db.zsetIndex.idx.RangeByScore(string(key), from, to)

	return res, nil
}

func (db *DB) ZTop(key []byte, n int) ([]interface{}, error) {

	// check
	if err := db.checkKeySize(key); err != nil {
		return nil, err
	}

	// lock
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	res := db.zsetIndex.idx.Top(string(key), n)

	return res, nil
}

func (db *DB) ZScore(key, member []byte) (bool, float64) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.idx.GetScore(string(key), string(member))
}

func (db *DB) ZCard(key []byte) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.idx.GetCard(string(key))
}

func (db *DB) ZIsMember(key, member []byte) bool {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.idx.MemberExist(string(key), string(member))
}
