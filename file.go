package CaskDB

import (
	"bytes"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"log"
	"os"
)

var (
	FileNameFormat = map[uint16]string{
		0: "%d.data.str",
		1: "%d.data.list",
		2: "%d.data.hash",
		3: "%d.data.set",
		4: "%d.data.zset",
	}

	FileNameSuffix = []string{
		"str",
		"list",
		"hash",
		"set",
		"zset",
	}
)

type File struct {
	id     uint32
	fd     *os.File
	mmap   mmap.MMap
	offset int64
}

// create a new db file, use mmap to write and read
func NewFile(path string, fileId uint32, dataType uint16, fileSize int64) (*File, error) {
	filepath := path + string(os.PathSeparator) + fmt.Sprintf(FileNameFormat[dataType], fileId)

	fd, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	// before Truncate, we got offset
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	f := &File{
		id:     fileId,
		fd:     fd,
		offset: fi.Size(),
	}

	// make sure the file size is a fixed value
	if err := fd.Truncate(fileSize); err != nil {
		return nil, err
	}

	// new mmap
	m, err := mmap.Map(fd, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	f.mmap = m

	return f, nil
}

// stop the mapping, and 'sync' means whether flush the data to disk
func (f *File) Close(sync bool) error {
	if f != nil && f.mmap != nil {
		if sync {
			if err := f.Sync(); err != nil {
				return err
			}
		}
		if err := f.mmap.Unmap(); err != nil {
			return err
		}
	}
	return nil
}

// flush data to disk
func (f *File) Sync() error {
	if f.mmap != nil {
		return f.mmap.Flush()
	}
	return nil
}

// read an entry from file
func (f *File) Read(offset int64) (*Entry, error) {

	// read header
	buf, err := f.ReadBuf(offset, EntryHeaderSize)
	if err != nil {
		return nil, err
	}

	if bytes.Compare(buf, make([]byte, EntryHeaderSize)) == 0 {
		return nil, ErrorEmptyHeader
	}

	// decode the header to an entry
	e, err := DecodeHeader(buf)
	if err != nil {
		return nil, err
	}

	// read key
	offset += EntryHeaderSize
	if e.keySize > 0 {
		if e.key, err = f.ReadBuf(offset, int64(e.keySize)); err != nil {
			return nil, err
		}
	}

	// read value
	offset += int64(e.keySize)
	if e.valueSize > 0 {
		if e.value, err = f.ReadBuf(offset, int64(e.valueSize)); err != nil {
			return nil, err
		}
	}

	// make sure that value is unmistakable
	if e.crc != crc32.ChecksumIEEE(e.value) {
		return nil, ErrorCrcCheck
	}

	return e, nil
}

// read something from mmap
func (f *File) ReadBuf(offset, n int64) ([]byte, error) {
	if offset+n > int64(len(f.mmap)) {
		return nil, ErrorReadOverFlow
	}
	buf := make([]byte, n)
	copy(buf, f.mmap[offset:])
	return buf, nil
}

// write entry to file
func (f *File) Write(e *Entry) error {

	// turn entry to binary code
	b, err := e.Encode()
	if err != nil {
		return err
	}

	if f.offset+int64(len(b)) > int64(len(f.mmap)) {
		log.Println("[Write entry error]")
		return ErrorWriteOverFlow
	}

	// mmap write to disk
	copy(f.mmap[f.offset:], b)
	f.offset += int64(e.Size())

	return nil
}