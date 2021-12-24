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
	if err := db.checkSize(key, k, v); err != nil {
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
	f := db.activeFiles[Hash]
	idx := &Index{
		fileId: f.id,
		offset: f.offset - int64(e.Size()),
	}
	db.hashIndex.idx.Put(string(key), string(k), idx)

	return nil
}

func (db *DB) HGet(key, k []byte) ([]byte, error) {

	// check
	if err := db.checkSize(key, k); err != nil {
		return nil, err
	}

	// lock
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	v, err := db.hGetVal(key, k)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (db *DB) hGetVal(key, k []byte) ([]byte, error) {

	// find from index
	v := db.hashIndex.idx.Get(string(key), string(k))
	if v == nil {
		return nil, ErrorKeyNotExist
	}
	idx := v.(*Index)

	// get value by index
	val, err := db.readValue(Hash, idx)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (db *DB) HDel(key, k []byte) error {

	// check size
	if err := db.checkSize(key, k); err != nil {
		return nil
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
