package Adele

type Indexer interface {
	Len() int
}

type Index struct {
	valueSize uint32
	fileId    uint32
	offset    int64
}

func (i *Index) Len() int {
	return 16 // 4 + 4 + 8 = 16 bytes
}

func (db *DB) buildStrIndex(e *Entry, idx *Index) {
	switch e.GetMarkType() {
	case StringSet:
		db.strIndex.idx.Put(e.key, idx)
	case StringRemove:
		db.strIndex.idx.Remove(e.key)
	}
}

// put entry info in memory index
func (db *DB) StoreStrIndex(e *Entry) error {
	f := db.activeFiles[String]
	idx := &Index{
		valueSize: e.valueSize,
		fileId:    f.id,
		offset:    f.offset - int64(e.Size()), // offset is the entry start position
	}
	db.strIndex.idx.Put(e.key, idx)
	return nil
}
