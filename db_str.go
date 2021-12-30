package CaskDB

import (
	"CaskDB/ds"
	"sync"
)

type StrIndex struct {
	mu *sync.RWMutex
	idx *ds.AVLTree
}

func NewStrIndex() *StrIndex {
	return &StrIndex{
		mu: &sync.RWMutex{},
		//idx: ds.NewSkipList(),
		idx: ds.NewAVLTree(),
	}
}

func (db *DB) Set(key, value []byte) error {

	// check kv size, make sure the size is within the range
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValSize(value); err != nil {
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
	e := NewEntry(key, value, Str, StrSet, 0)

	// write to disk in entry
	if err := db.StoreFile(e); err != nil {
		return err
	}

	// write to memory index
	f := db.activeFiles[Str]
	idx := &Index{
		//valueSize: e.valueSize,
		fileId: f.id,
		offset: f.offset - int64(e.Size()), // offset is the entry start position
	}
	db.strIndex.idx.Put(e.key, idx)

	return nil
}

// set if not exist
func (db *DB) SetNx(key, value []byte) error {

	// check
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkValSize(value); err != nil {
		return err
	}

	// lock
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	if err := db.setValNx(key, value); err != nil {
		return err
	}

	return nil
}

func (db *DB) setValNx(key, value []byte) error {
	v, err := db.getVal(key)
	if err != nil {
		return err
	}
	if v == nil {
		if err = db.setVal(key, value); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {

	// check key size
	if err := db.checkKeySize(key); err != nil {
		return nil, err
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	v, err := db.getVal(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// get key from Adele
func (db *DB) getVal(key []byte) ([]byte, error) {

	// get index and find value from disk
	v := db.strIndex.idx.Get(key)
	if v == nil {
		return nil, nil
	}
	idx := v.(*Index)

	// read value by index
	val, err := db.readValue(Str, idx)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// set new value and return old value
func (db *DB) GetSet(key, value []byte) ([]byte, error) {

	// check size
	if err := db.checkKeySize(key); err != nil {
		return nil, err
	}
	if err := db.checkValSize(value); err != nil {
		return nil, err
	}

	// lock
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	v, err := db.getVal(key)
	if err != nil {
		return nil, err
	}
	if err = db.setVal(key, value); err != nil {
		return nil, err
	}

	return v, nil
}

func (db *DB) MSet(values ...[]byte) error {

	// check
	if values == nil {
		return ErrorNilPointer
	}
	if len(values)%2 != 0 {
		return ErrorMSetParams
	}

	// lock
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for i := 0; i < len(values); i += 2 {
		k, v := values[i], values[i+1]
		if err := db.checkKeySize(k); err != nil {
			return err
		}
		if err := db.checkValSize(v); err != nil {
			return err
		}
		if err := db.setVal(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) MGet(keys ...[]byte) ([][]byte, error) {

	// check
	if err := db.checkKeysSize(keys...); err != nil {
		return nil, err
	}

	// lock
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	var res [][]byte
	for _, k := range keys {
		v, err := db.getVal(k)
		if err != nil {
			return nil, err
		}
		res = append(res, v)
	}
	return res, nil
}

func (db *DB) MSetNx(values ...[]byte) error {
	// check
	if values == nil {
		return ErrorNilPointer
	}
	if len(values)%2 != 0 {
		return ErrorMSetParams
	}

	// lock
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for i := 0; i < len(values); i += 2 {
		k, v := values[i], values[i+1]
		if err := db.checkKeySize(k); err != nil {
			return err
		}
		if err := db.checkValSize(v); err != nil {
			return err
		}
		if err := db.setValNx(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Remove(key []byte) error {

	// check key
	if err := db.checkKeySize(key); err != nil {
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
	e := NewEntry(key, nil, Str, StrRemove, 0)

	// append write entry, type of remove
	if err := db.StoreFile(e); err != nil {
		return err
	}

	// remove index
	db.strIndex.idx.Remove(key)

	return nil
}

// get str size
func (db *DB) StrLen() int {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	return db.strIndex.idx.Size()
}
