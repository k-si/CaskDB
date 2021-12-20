package CaskDB

import (
	"CaskDB/util"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrorKeyEmpty       = errors.New("[the size of key can not be 0]")
	ErrorKeySizeLimit   = errors.New("[key size larger than max]")
	ErrorKeyNotExist    = errors.New("[this key not in DB]")
	ErrorValueSizeLimit = errors.New("[value size larger than max]")
	ErrorWriteOverFlow  = errors.New("[entry too large so that the file can not storage]")
	ErrorReadOverFlow   = errors.New("[read file offset overflow]")
	ErrorNotInArch      = errors.New("[not found this file in arched files]")
	ErrorCrcCheck       = errors.New("[crc check value is not the same as before]")
	ErrorClosedDB       = errors.New("[using a closed DB]")
	ErrorMergingMerge   = errors.New("[start merge when db is merging]")
	ErrorEmptyHeader    = errors.New("[read an empty entry header, maybe read 0]")
	ErrorNilMergedFile  = errors.New("[active merged file nil]")
)

const (
	MergeDirName  = "merged"
	PathSeparator = string(os.PathSeparator)
	DataTypeNum = 5
)

type DB struct {
	config Config

	activeFiles []*File            // activeFiles[dataType] = file
	archedFiles []map[uint32]*File // archedFiles[dataType] = fileId -> file
	strIndex    *StrIndex

	isMerging uint32 // 0: not merge 1: merging
	isClosed  uint32 // 0: not close 1: closed
	mergeChan chan struct{}
}

// get a DB instance
func Open(config Config) (*DB, error) {

	// check db path
	if err := util.CheckAndMakeDir(config.DBDir); err != nil {
		return nil, err
	}

	// check merged path
	if err := util.CheckAndMakeDir(config.DBDir + PathSeparator + MergeDirName); err != nil {
		return nil, err
	}

	db := &DB{
		config:    config,
		strIndex:  NewStrIndex(),
		mergeChan: make(chan struct{}, DataTypeNum),
	}

	// load db files fd from disk
	// fids is dataType->fileId array, and every type mapped sorted array
	activeFiles, archedFiles, fids, err := db.loadFiles()
	if err != nil {
		return nil, err
	}
	db.activeFiles = activeFiles
	db.archedFiles = archedFiles

	// todo: load transaction

	// load memory indexes
	if err := db.loadIndexes(fids); err != nil {
		return nil, err
	}

	// start timed merge goroutine
	go db.gc()

	return db, nil
}


func (db *DB) Close() error {

	// check status
	if atomic.LoadUint32(&db.isClosed) == 1 {
		return ErrorClosedDB
	}
	if atomic.LoadUint32(&db.isMerging) == 1 {
		if err := db.StopGc(); err != nil {
			return err
		}
	}

	// todo: save configuration

	// close fd
	for i := 0; i < DataTypeNum; i++ {
		if err := db.activeFiles[i].Close(true); err != nil {
			return err
		}
		for _, f := range db.archedFiles[i] {
			if err := f.Close(true); err != nil {
				return err
			}
		}
	}

	atomic.StoreUint32(&db.isClosed, 1)

	return nil
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
					valueSize: e.valueSize,
					fileId:    f.id,
					offset:    offset,
				}

				// different data types correspond to different index types
				// todo: other types
				switch e.GetDataType() {
				case String:
					db.buildStrIndex(e, idx)
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

// traverse all files and extract their fds
func (db *DB) loadFiles() ([]*File, []map[uint32]*File, map[int][]int, error) {
	dataTypeIds, err := loadFilesId(db.config.DBDir)
	if err != nil {
		return nil, nil, nil, err
	}

	activeFiles := make([]*File, DataTypeNum)
	archedFiles := make([]map[uint32]*File, DataTypeNum)

	for i := 0; i < DataTypeNum; i++ {

		// sort ids, maximum id of active file
		sort.Ints(dataTypeIds[i])
		ids := dataTypeIds[i]

		archedFiles[i] = make(map[uint32]*File)

		// 0th - (n-2)th is arched file id
		for j := 0; j < len(ids)-1; j++ {
			f, err := NewFile(db.config.DBDir, uint32(ids[j]), uint16(i), db.config.MaxFileSize)
			if err != nil {
				return nil, nil, nil, err
			}
			archedFiles[i][uint32(ids[j])] = f
		}

		// (n-1)th is active file id
		var id uint32
		if len(ids)-1 >= 0 {
			id = uint32(ids[len(ids)-1])
		}

		f, err := NewFile(db.config.DBDir, id, uint16(i), db.config.MaxFileSize)
		if err != nil {
			log.Println("[NewFile err]", err)
			return nil, nil, nil, err
		}
		activeFiles[i] = f
	}

	return activeFiles, archedFiles, dataTypeIds, nil
}

// load all files id in map
func loadFilesId(dir string) (map[int][]int, error) {
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	// collect files id
	// dataType -> [id, id...]
	dataTypeIds := make(map[int][]int)
	for _, i := range infos {
		if strings.Contains(i.Name(), ".data") {
			names := strings.Split(i.Name(), ".") // ["000" "data" "str"]
			fid, err := strconv.Atoi(names[0])
			if err != nil {
				return nil, err
			}
			switch names[2] {
			case FileNameSuffix[0]:
				dataTypeIds[0] = append(dataTypeIds[0], fid)
			case FileNameSuffix[1]:
				dataTypeIds[1] = append(dataTypeIds[1], fid)
			case FileNameSuffix[2]:
				dataTypeIds[2] = append(dataTypeIds[2], fid)
			case FileNameSuffix[3]:
				dataTypeIds[3] = append(dataTypeIds[3], fid)
			case FileNameSuffix[4]:
				dataTypeIds[4] = append(dataTypeIds[4], fid)
			}
		}
	}

	return dataTypeIds, nil
}

func (db *DB) getArchedFile(dataType uint16, fileId uint32) (*File, error) {
	f, ok := db.archedFiles[dataType][fileId]
	if !ok {
		return nil, ErrorNotInArch
	}
	return f, nil
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

// write entry to disk
func (db *DB) StoreFile(e *Entry) error {
	f := db.activeFiles[e.GetDataType()]

	// check active file size
	if f.offset+int64(e.Size()) > db.config.MaxFileSize {

		// flush current active file to disk
		if err := f.Sync(); err != nil {
			return err
		}

		// create new file as active file
		newId := f.id + 1
		newf, err := NewFile(db.config.DBDir, newId, e.GetDataType(), db.config.MaxFileSize)
		if err != nil {
			return err
		}
		db.archedFiles[e.GetDataType()][f.id] = f
		db.activeFiles[e.GetDataType()] = newf
		f = newf
	}

	// write entry in active file
	if err := f.Write(e); err != nil {
		return err
	}

	// sync buffer with disk
	if db.config.WriteSync {
		if err := f.Sync(); err != nil {
			return err
		}
	}

	return nil
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
		db.strIndex.idx.Put(e.key, idx)
	}

	// sync buffer with disk
	if err := (*activeFile).Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) checkSize(k, v []byte) error {
	if k != nil {
		if len(k) == 0 {
			return ErrorKeyEmpty
		}
		if uint32(len(k)) > db.config.MaxKeySize {
			return ErrorKeySizeLimit
		}
	}
	if v != nil && uint32(len(v)) > db.config.MaxValueSize {
		return ErrorValueSizeLimit
	}
	return nil
}
