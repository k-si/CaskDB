package CaskDB

import (
	"CaskDB/ds"
	"sync"
)

type SetIndex struct {
	mu  *sync.RWMutex
	idx *ds.Set
}

func NewSetIndex() *SetIndex {
	return &SetIndex{
		mu:  &sync.RWMutex{},
		idx: ds.NewSet(),
	}
}

func (db *DB) SAdd(key []byte, values ...[]byte) error {

	// check size
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValsSize(values...); err != nil {
		return err
	}

	// lock
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, v := range values {

		// write disk
		e := NewEntry(key, v, Set, SetSAdd, 0)
		if err := db.StoreFile(e); err != nil {
			return nil
		}

		// index
		db.setIndex.idx.Add(string(key), string(v))
	}

	return nil
}

func (db *DB) SRem(key, value []byte) error {

	// check size
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValSize(value); err != nil {
		return err
	}

	// lock
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	// store disk
	e := NewEntry(key, value, Set, SetSRem, 0)
	if err := db.StoreFile(e); err != nil {
		return err
	}

	// store index
	db.setIndex.idx.Remove(string(key), string(value))

	return nil
}

func (db *DB) SMove(src, dest, value []byte) error {

	// check size
	if err := db.checkKeysSize(src, dest); err != nil {
		return err
	}
	if err := db.checkValSize(value); err != nil {
		return err
	}

	// lock
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	// store disk
	keys := db.splice(src, dest)
	e := NewEntry(keys, value, Set, SetSMove, uint32(len(src)))
	if err := db.StoreFile(e); err != nil {
		return err
	}

	// store index
	db.setIndex.idx.Move(string(src), string(dest), string(value))

	return nil
}

func (db *DB) SUnion(keys ...[]byte) ([][]byte, error) {

	// check
	if err := db.checkKeysSize(keys...); err != nil {
		return nil, err
	}

	// lock
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	// store index
	ks := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		ks[i] = string(keys[i])
	}
	values := db.setIndex.idx.Union(ks...)

	// return union
	res := make([][]byte, len(values))
	for i := 0; i < len(values); i++ {
		res[i] = []byte(values[i])
	}
	return res, nil
}

// set1 = a, b
// set2 = a, b, c, d
// SDiff(s1, s2) = c, d
func (db *DB) SDiff(keys ...[]byte) ([][]byte, error) {

	// check size
	if err := db.checkKeysSize(keys...); err != nil {
		return nil, err
	}

	// lock
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	// store index
	ks := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		ks[i] = string(keys[i])
	}
	values := db.setIndex.idx.Diff(ks...)

	// return diff
	res := make([][]byte, len(values))
	for i := 0; i < len(values); i++ {
		res[i] = []byte(values[i])
	}
	return res, nil
}

// get all members
func (db *DB) SScan(key []byte) ([][]byte, error) {

	// check
	if err := db.checkKeySize(key); err != nil {
		return nil, err
	}

	// lock
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	res := db.setIndex.idx.Scan(string(key))
	return res, nil
}

func (db *DB) SIsMember(key, value []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	b := db.setIndex.idx.ValExist(string(key), string(value))
	return b
}

func (db *DB) SCard(key []byte) int {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	return db.setIndex.idx.Len(string(key))
}
