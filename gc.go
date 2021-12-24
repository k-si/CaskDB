package CaskDB

import (
	"CaskDB/util"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func (db *DB) listeningGC() {
	timer := time.NewTimer(db.config.MergeInterval)
	defer timer.Stop()

	select {
	case <-timer.C:
		timer.Reset(db.config.MergeInterval)
		if err := db.GC(); err != nil {
			log.Println("[merge err]", err)
			return
		}
	}
}

// garbage file recycling
// merge all files and remove useless data
// todo: backpack and rollback
func (db *DB) GC() error {

	// check status
	if atomic.LoadUint32(&db.isClosed) == 1 {
		return ErrorClosedDB
	}
	if atomic.LoadUint32(&db.isMerging) == 1 {
		return ErrorMergingMerge
	}

	log.Println("[stop the world]")
	db.strIndex.mu.Lock()
	db.hashIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	defer db.hashIndex.mu.Unlock()
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

			mergedArchedFiles := make(map[uint32]*File)
			var mergedActiveFile *File

			for j := 0; j < len(ids); j++ {
				select {
				case <-db.mergeChan:
					log.Println("[exit merge task]", i)
					return
				default:
					f, err := db.getFileById(uint16(i), uint32(ids[j]))
					if err != nil {
						return
					}

					// read all entry from files, but except empty file
					var offset int64
					if offset == f.offset {
						continue
					}
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
					if mergeErr = f.Close(true); mergeErr != nil {
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
			db.activeFiles[i] = mergedActiveFile
			db.archedFiles[i] = mergedArchedFiles

			// rename new merged file
			fi, _ := mergedActiveFile.fd.Stat()
			name := PathSeparator + fi.Name()
			if mergeErr = os.Rename(mergePath+name, db.config.DBDir+name); mergeErr != nil {
				return
			}
			for _, f := range db.archedFiles[i] {
				fi, _ = f.fd.Stat()
				name = PathSeparator + fi.Name()
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

func (db *DB) StopGC() error {

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

// clear the files in merged directory
func (db *DB) removeMergedFiles() error {
	mergedPath := db.config.DBDir + PathSeparator + MergeDirName
	mInfos, err := ioutil.ReadDir(mergedPath)
	if err != nil {
		return err
	}
	for _, mi := range mInfos {
		fp := mergedPath + PathSeparator + mi.Name()
		if err := os.Remove(fp); err != nil {
			return err
		}
	}
	return nil
}

// todo: other types
// while merging, need 'set' or 'update' type of entry, 'remove' type is useless
func (db *DB) entryValid(e *Entry, eFid uint32, eOffset int64) bool {
	if e == nil {
		return false
	}

	switch e.GetDataType() {
	case String:
		if e.GetMarkType() == StringSet {

			// entry is valid, if key, file id, offset all equals index
			v := db.strIndex.idx.Get(e.key)
			return db.checkPosition(v, eFid, eOffset)
		}
	case List:
		mt := e.GetMarkType()
		if mt == ListLPush || mt == ListRPush {
			record := db.listIndex.idx.GetRecord()
			v := LIdxGet(record, string(e.key), e.value)
			return db.checkPosition(v, eFid, eOffset)
		}
	case Hash:
		if e.GetMarkType() == HashHSet {
			v := db.hashIndex.idx.Get(e.GetPreKey(), e.GetPostKey())
			return db.checkPosition(v, eFid, eOffset)
		}
	}

	return false
}

func (db *DB) checkPosition(v interface{}, eFid uint32, eOffset int64) bool {
	if v == nil {
		return false
	}
	idx := v.(*Index)
	if eFid == idx.fileId && eOffset == idx.offset {
		return true
	}
	return false
}

// store entry in merged files
// we should use *activeFile
func (db *DB) storeMerged(e *Entry, archedFiles map[uint32]*File, activeFile **File) error {
	mergePath := db.config.DBDir + PathSeparator + MergeDirName

	// init
	if (*activeFile) == nil {
		f, err := NewFile(mergePath, 0, e.GetDataType(), db.config.MaxFileSize)
		if err != nil {
			return err
		}
		*activeFile = f
	}

	// check active file size
	if (*activeFile).offset+int64(e.Size()) > db.config.MaxFileSize {

		// flush current active file to disk
		if err := (*activeFile).Sync(); err != nil {
			return err
		}

		// create new file as active file
		newId := (*activeFile).id + 1
		newf, err := NewFile(mergePath, newId, e.GetDataType(), db.config.MaxFileSize)
		if err != nil {
			return err
		}
		archedFiles[(*activeFile).id] = *activeFile
		*activeFile = newf
	}

	// write entry in active file
	if err := (*activeFile).Write(e); err != nil {
		return err
	}

	// update indexes
	// todo: other types
	switch e.GetDataType() {
	case String:
		idx := db.strIndex.idx.Get(e.key).(*Index)
		idx.fileId = (*activeFile).id
		idx.offset = (*activeFile).offset - int64(e.Size())
	case List:
		// do noting...
	case Hash:
		idx := db.hashIndex.idx.Get(e.GetPreKey(), e.GetPostKey()).(*Index)
		idx.fileId = (*activeFile).id
		idx.offset = (*activeFile).offset - int64(e.Size())
	}

	// sync buffer with disk
	if err := (*activeFile).Sync(); err != nil {
		return err
	}

	return nil
}
