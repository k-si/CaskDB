package CaskDB

import (
	"CaskDB/ds"
	"sync"
)

type HashIndex struct {
	mu  *sync.RWMutex
	idx *ds.Hash
}

func NewHashIndex() *HashIndex {
	return &HashIndex{
		mu:  &sync.RWMutex{},
		idx: ds.NewHash(),
	}
}

func (db *DB) HSet(key, k, v []byte) error {

	// check size
	if err := db.checkKeysSize(key, k); err != nil {
		return err
	}
	if err := db.checkValSize(v); err != nil {
		return err
	}

	// lock
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if err := db.hSetVal(key, k, v); err != nil {
		return err
	}

	return nil
}

func (db *DB) hSetVal(key, k, v []byte) error {

	// store file
	// keys = key | k
	keys := db.splice(key, k)
	e := NewEntry(keys, v, Hash, HashHSet, uint32(len(key)))

	if err := db.StoreFile(e); err != nil {
		return err
	}

	// store index
	db.hashIndex.idx.Put(string(key), string(k), v)

	return nil
}

func (db *DB) HGet(key, k []byte) ([]byte, error) {

	// check
	if err := db.checkKeysSize(key, k); err != nil {
		return nil, err
	}

	// lock
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	v := db.hashIndex.idx.Get(string(key), string(k))

	return v, nil
}

func (db *DB) HDel(key, k []byte) error {

	// check size
	if err := db.checkKeysSize(key, k); err != nil {
		return err
	}

	// lock
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if err := db.hDelVal(key, k); err != nil {
		return err
	}

	return nil
}

func (db *DB) hDelVal(key, k []byte) error {

	// append entry of delete
	keys := db.splice(key, k)
	e := NewEntry(keys, nil, Hash, HashHDel, uint32(len(key)))
	if err := db.StoreFile(e); err != nil {
		return err
	}

	// delete from index
	db.hashIndex.idx.Remove(string(key), string(k))
	return nil
}

func (db *DB) HSetNx(key, k, v []byte) error {

	// check size
	if err := db.checkKeysSize(key, k); err != nil {
		return err
	}
	if err := db.checkValSize(v); err != nil {
		return err
	}

	// lock
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	val := db.hashIndex.idx.Get(string(key), string(k))
	if val == nil {
		if err := db.hSetVal(key, k, v); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) HGetAll(key []byte) ([][]byte, error) {

	// check size
	if err := db.checkKeySize(key); err != nil {
		return nil, err
	}

	// lock
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	res := db.hashIndex.idx.GetAll(string(key))
	return res, nil
}

func (db *DB) HExist(key, k []byte) bool {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	return db.hashIndex.idx.FieldExist(string(key), string(k))
}

func (db *DB) HLen(key []byte) int {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	return db.hashIndex.idx.Len(string(key))
}
