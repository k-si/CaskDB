package CaskDB

import (
	"CaskDB/ds"
	"CaskDB/util"
	"sync"
)

type Index struct {
	//valueSize uint32
	fileId uint32
	offset int64
	value  []byte
}

func (i *Index) Value() []byte {
	return i.value
}

type (
	SetIndex struct {
		mu  *sync.RWMutex
		idx *ds.Set
	}
)

func NewSetIndex() *SetIndex {
	return &SetIndex{
		mu:  &sync.RWMutex{},
		idx: ds.NewSet(),
	}
}

// build index when starting database
func (db *DB) buildStrIndex(e *Entry, idx *Index) {
	switch e.GetMarkType() {
	case StringSet:
		db.strIndex.idx.Put(e.key, idx)
	case StringRemove:
		db.strIndex.idx.Remove(e.key)
	}
}

func (db *DB) buildListIndex(e *Entry, idx *Index) {
	switch e.GetMarkType() {
	case ListLPush:
		db.listIndex.idx.Push(true, string(e.key), idx)
	case ListRPush:
		db.listIndex.idx.Push(false, string(e.key), idx)
	case ListLPop:
		db.listIndex.idx.Pop(true, string(e.key))
	case ListRPop:
		db.listIndex.idx.Pop(false, string(e.key))
	case ListLRem:
		record := db.listIndex.idx.GetRecord()
		n := util.BytesToInt(e.GetPostBytesKey())
		LIdxRem(record, e.GetPreKey(), e.value, n)
	}
}

func (db *DB) buildHashIndex(e *Entry, idx *Index) {
	switch e.GetMarkType() {
	case HashHSet:
		db.hashIndex.idx.Put(e.GetPreKey(), e.GetPostKey(), idx)
	case HashHDel:
		db.hashIndex.idx.Remove(e.GetPreKey(), e.GetPostKey())
	}
}

// traverse all the content of files, modify the index in memory
// according to the data operation type
func (db *DB) loadIndexes(fids map[int][]int) (err error) {
	var loadErr error

	wg := sync.WaitGroup{}

	// every goroutine do its type
	for i := 0; i < DataTypeNum; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			ids := fids[i]

			// traverse arched files
			for j := 0; j < len(ids)-1; j++ {
				af, err := db.getArchedFile(uint16(i), uint32(ids[j]))
				if err != nil {
					loadErr = err
					return
				}
				if err := db.loadFileIndexes(af); err != nil {
					loadErr = err
					return
				}
			}

			// traverse active files
			f := db.activeFiles[i]
			if err := db.loadFileIndexes(f); err != nil {
				loadErr = err
				return
			}
		}(i)
	}

	wg.Wait()

	if loadErr != nil {
		return loadErr
	}

	return
}

func (db *DB) loadFileIndexes(f *File) error {

	// there may be no files at the beginning
	// Pay attention to null pointers when loading
	if f != nil {

		var offset int64

		// read entry from file
		for offset < db.config.MaxFileSize {
			if e, err := f.Read(offset); err == nil {
				idx := &Index{
					//valueSize: e.valueSize,
					fileId: f.id,
					offset: offset,
				}

				// different data types correspond to different index types
				// todo: other types
				switch e.GetDataType() {
				case String:
					db.buildStrIndex(e, idx)
				case List:
					db.buildListIndex(e, idx)
				case Hash:
					db.buildHashIndex(e, idx)
				}
				offset += int64(e.Size())
			} else if err == ErrorEmptyHeader {

				// the file is full of 0
				// reading an empty header means reading to the end
				f.offset = offset

				// read to end, then break
				break
			} else {
				return err
			}
		}
	}

	return nil
}
