package CaskDB

func (db *DB) StrKeyExist(key []byte) bool {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	v := db.strIndex.idx.Get(key)
	if v == nil {
		return false
	}
	return true
}

func (db *DB) HKeyExist(key []byte) bool {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	return db.hashIndex.idx.KeyExist(string(key))
}

func (db *DB) LKeyExist(key []byte) bool {
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	return db.listIndex.idx.KeyExist(string(key))
}

func (db *DB) SKeyExist(key []byte) bool {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	return db.setIndex.idx.KeyExist(string(key))
}

func (db *DB) ZKeyExist(key []byte) bool {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	return db.zsetIndex.idx.KeyExist(string(key))
}
