package CaskDB

import (
	"CaskDB/ds"
	"sync"
)

type StrIndex struct {
	mu  *sync.RWMutex
	idx *ds.SkipList
}

func NewStrIndex() *StrIndex {
	return &StrIndex{
		mu:  &sync.RWMutex{},
		idx: ds.NewSkipList(),
	}
}

func (db *DB) Set(key, value []byte) error {

	// check kv size, make sure the size is within the range
	if err := db.checkSize(key, value); err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if err := db.setVal(key, value); err != nil {
		return err
	}
	return nil
}

// set kv in disk and memory
func (db *DB) setVal(key, value []byte) error {
	e := NewEntry(key, value, String, StringSet)

	// write to disk in entry
	if err := db.StoreFile(e); err != nil {
		return err
	}

	// write to memory index
	if err := db.StoreStrIndex(e); err != nil {
		return err
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {

	// check key size
	if err := db.checkSize(key, nil); err != nil {
		return nil, err
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	value, err := db.getVal(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// get key from Adele
func (db *DB) getVal(key []byte) ([]byte, error) {

	// get index and find value from disk
	v := db.strIndex.idx.Get(key)
	if v == nil {
		return nil, ErrorKeyNotExist
	}
	idx := v.(*Index)

	// find entry from disk
	f := db.activeFiles[String]
	if f.id != idx.fileId {
		af, err := db.getArchedFile(String, idx.fileId)
		if err != nil {
			return nil, err
		}
		f = af
	}
	e, err := f.Read(idx.offset)
	if err != nil {
		return nil, err
	}

	return e.value, nil
}

func (db *DB) Remove(key []byte) error {

	// check key
	if err := db.checkSize(key, nil); err != nil {
		return err
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if err := db.removeVal(key); err != nil {
		return err
	}
	return nil
}

// remove a kv from memory and disk
func (db *DB) removeVal(key []byte) error {
	e := NewEntry(key, nil, String, StringRemove)

	// append write entry, type of remove
	if err := db.StoreFile(e); err != nil {
		return err
	}

	// remove index
	db.strIndex.idx.Remove(key)

	return nil
}
