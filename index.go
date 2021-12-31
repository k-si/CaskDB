package CaskDB

import (
	"github.com/k-si/CaskDB/ds"
	"github.com/k-si/CaskDB/util"
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

// build index when starting database
func (db *DB) buildStrIndex(e *Entry, idx *Index) {
	switch e.GetMarkType() {
	case StrSet:
		db.strIndex.idx.Put(e.key, idx)
	case StrRemove:
		db.strIndex.idx.Remove(e.key)
	}
}

func (db *DB) buildListIndex(e *Entry) {
	switch e.GetMarkType() {
	case ListLPush:
		db.listIndex.idx.Push(true, string(e.key), e.value)
	case ListLPop:
		db.listIndex.idx.Pop(true, string(e.key))
	case ListRPush:
		db.listIndex.idx.Push(false, string(e.key), e.value)
	case ListRPop:
		db.listIndex.idx.Pop(false, string(e.key))
	case ListLSet:
		n := util.BytesToInt(e.GetPostBytesKey())
		db.listIndex.idx.Put(e.GetPreKey(), e.value, n)
	case ListLInsert:
		n := util.BytesToInt(e.GetPostBytesKey())
		db.listIndex.idx.Insert(e.GetPreKey(), ds.Before, n, e.value)
	case ListRInsert:
		n := util.BytesToInt(e.GetPostBytesKey())
		db.listIndex.idx.Insert(e.GetPreKey(), ds.After, n, e.value)
	case ListLRem:
		n := util.BytesToInt(e.GetPostBytesKey())
		db.listIndex.idx.Remove(e.GetPreKey(), e.value, n)
	}
}

func (db *DB) buildHashIndex(e *Entry) {
	switch e.GetMarkType() {
	case HashHSet:
		db.hashIndex.idx.Put(e.GetPreKey(), e.GetPostKey(), e.value)
	case HashHDel:
		db.hashIndex.idx.Remove(e.GetPreKey(), e.GetPostKey())
	}
}

func (db *DB) buildSetIndex(e *Entry) {
	switch e.GetMarkType() {
	case SetSAdd:
		db.setIndex.idx.Add(string(e.key), string(e.value))
	case SetSRem:
		db.setIndex.idx.Remove(string(e.key), string(e.value))
	case SetSMove:
		db.setIndex.idx.Move(e.GetPreKey(), e.GetPostKey(), string(e.value))
	}
}

func (db *DB) buildZSetIndex(e *Entry) {
	switch e.GetMarkType() {
	case ZSetZAdd:
		score := util.BytesToFloat64(e.GetPostBytesKey())
		db.zsetIndex.idx.Add(e.GetPreKey(), string(e.value), score)
	case ZSetZRem:
		db.zsetIndex.idx.Remove(string(e.key), string(e.value))
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

				// different data types correspond to different index types
				switch e.GetDataType() {
				case Str:
					idx := &Index{
						fileId: f.id,
						offset: offset,
					}
					db.buildStrIndex(e, idx)
				case List:
					db.buildListIndex(e)
				case Hash:
					db.buildHashIndex(e)
				case Set:
					db.buildSetIndex(e)
				case ZSet:
					db.buildZSetIndex(e)
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
