package CaskDB

import (
	"CaskDB/util"
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
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
	DataTypeNum   = 5
)

type DB struct {
	config Config

	activeFiles []*File            // activeFiles[dataType] = file
	archedFiles []map[uint32]*File // archedFiles[dataType] = fileId -> file
	strIndex    *StrIndex
	listIndex   *ListIndex
	hashIndex   *HashIndex
	setIndex    *SetIndex

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
		listIndex: NewListIndex(),
		hashIndex: NewHashIndex(),
		setIndex:  NewSetIndex(),
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
	go db.listeningGC()

	return db, nil
}

func (db *DB) Close() error {

	// check status
	if atomic.LoadUint32(&db.isClosed) == 1 {
		return ErrorClosedDB
	}
	if atomic.LoadUint32(&db.isMerging) == 1 {
		if err := db.StopGC(); err != nil {
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

// check size
func (db *DB) checkSize(key, k []byte, v ...[]byte) error {
	if err := db.checkKeySize(key); err != nil {
		return err
	}
	if err := db.checkKeySize(k); err != nil {
		return err
	}
	for _, i := range v {
		if err := db.checkValSize(i); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) checkKeySize(key []byte) error {
	if key != nil {
		if len(key) == 0 {
			return ErrorKeyEmpty
		}
		if uint32(len(key)) > db.config.MaxKeySize {
			return ErrorKeySizeLimit
		}
	}
	return nil
}

func (db *DB) checkValSize(val []byte) error {
	if val != nil {
		if uint32(len(val)) > db.config.MaxValueSize {
			return ErrorValueSizeLimit
		}
	}
	return nil
}

// splice two bytes in a []byte
func (db *DB) splice(k1, k2 []byte) []byte {
	var buf bytes.Buffer
	buf.Write(k1)
	buf.Write(k2)
	return buf.Bytes()
}

func (db *DB) getArchedFile(dataType uint16, fileId uint32) (*File, error) {
	f, ok := db.archedFiles[dataType][fileId]
	if !ok {
		return nil, ErrorNotInArch
	}
	return f, nil
}

func (db *DB) getFileById(dataType uint16, fileId uint32) (*File, error) {
	f := db.activeFiles[dataType]
	if f.id != fileId {
		af, err := db.getArchedFile(dataType, fileId)
		if err != nil {
			return nil, err
		}
		f = af
	}
	return f, nil
}

func (db *DB) readValue(dataType uint16, idx *Index) ([]byte, error) {
	f, err := db.getFileById(dataType, idx.fileId)
	if err != nil {
		return nil, err
	}

	val, err := f.ReadValue(idx.offset)
	if err != nil {
		return nil, err
	}
	return val, nil
}
