package Adele

import (
	"Adele/util"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func (db *DB) gc() {
	timer := time.NewTimer(db.config.MergeInterval)
	defer timer.Stop()

	select {
	case <-timer.C:
		timer.Reset(db.config.MergeInterval)
		if err := db.StartMerge(); err != nil {
			log.Println("[merge err]", err)
			return
		}
	}
}

// garbage file recycling
// merge all files and remove useless data
// todo: backpack and rollback
func (db *DB) StartMerge() error {

	// check status
	if atomic.LoadUint32(&db.isClosed) == 1 {
		return ErrorClosedDB
	}
	if atomic.LoadUint32(&db.isMerging) == 1 {
		return ErrorMergingMerge
	}

	log.Println("[stop the world]")
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	// todo: lock every index lock

	// change status
	atomic.StoreUint32(&db.isMerging, 1)
	defer atomic.StoreUint32(&db.isMerging, 0)

	// check merged path
	mergePath := db.config.DBDir + PathSeparator + MergeDirName
	if err := util.CheckAndMakeDir(mergePath); err != nil {
		return err
	}

	// all files that fail to merge will be deleted
	if err := db.removeMergedFiles(); err != nil {
		return err
	}

	// load all files id
	fids, err := loadFilesId(db.config.DBDir)
	if err != nil {
		return err
	}

	var mergeErr error

	wg := sync.WaitGroup{}
	for i := 0; i < DataTypeNum; i++ {
		wg.Add(1)

		// every goroutine do its merge task
		go func(i int) {
			defer wg.Done()

			ids := fids[i] // all files with this type

			// only merge arched file
			if len(ids) < 2 {
				return
			}

			mergedArchedFiles := make(map[uint32]*File)
			var mergedActiveFile *File

			// one type
			// 0 - (n-2) is arched file
			sort.Ints(ids)
			for j := 0; j < len(ids)-1; j++ {
				select {
				case <-db.mergeChan:
					log.Println("[exit merge task]", i)
					return
				default:
					f, err := db.getArchedFile(uint16(i), uint32(ids[j]))
					if err != nil {
						return
					}

					// read all entry from files
					var offset int64
					for offset < f.offset {
						e, err := f.Read(offset)
						if err != nil {
							mergeErr = err
							return
						}

						// check entry valid
						if ok := db.entryValid(e, uint32(ids[j]), offset); ok {

							// store entry
							// here use &, make mergedActiveFile modifiable
							if mergeErr = db.storeMerged(e, mergedArchedFiles, &mergedActiveFile); mergeErr != nil {
								return
							}
						}
						offset += int64(e.Size())
					}

					// close and remove old file
					if mergeErr = f.Close(false); mergeErr != nil {
						return
					}
					if mergeErr = os.Remove(f.fd.Name()); mergeErr != nil {
						return
					}
				}
			}
			// update archedFiles
			if mergedActiveFile == nil {
				mergeErr = ErrorNilMergedFile
				return
			}
			mergedArchedFiles[mergedActiveFile.id] = mergedActiveFile
			db.archedFiles[i] = mergedArchedFiles

			// rename new merged file
			for _, f := range db.archedFiles[i] {
				fi, _ := f.fd.Stat()
				name := PathSeparator + fi.Name()
				if mergeErr = os.Rename(mergePath+name, db.config.DBDir+name); mergeErr != nil {
					return
				}
			}

			// update indexes
			// i can load all files again, but i choose updating in func storeMerged

		}(i)
	}
	wg.Wait()

	if mergeErr != nil {
		// todo: rollback
		return mergeErr
	}

	return nil
}

func (db *DB) StopGc() error {

	// check status
	if atomic.LoadUint32(&db.isClosed) == 1 {
		return ErrorClosedDB
	}

	// channel notify
	if atomic.LoadUint32(&db.isMerging) == 1 {
		go func() {
			for i := 0; i < DataTypeNum; i++ {
				db.mergeChan <- struct{}{}
			}
		}()
	}
	return nil
}

// todo: other types
// while merging, need 'set' type of entry, 'remove' type is useless
func (db *DB) entryValid(e *Entry, eFid uint32, eOffset int64) bool {
	if e == nil {
		return false
	}

	switch e.GetDataType() {
	case String:
		if e.GetMarkType() == StringSet {

			// entry is valid, if key, file id, offset all equals index
			n := db.strIndex.idx.Get(e.key)
			if n == nil {
				return false
			}
			idx := n.Value().(*Index)
			if eFid == idx.fileId && eOffset == idx.offset {
				return true
			}
		}
	}

	return false
}
