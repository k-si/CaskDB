package CaskDB

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const EntryHeaderSize = 26

// data type
const (
	String = iota
	List
	Hash
)

// mark type
const (
	StringSet uint16 = iota
	StringRemove
)

const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
)

const (
	HashHSet uint16 = iota
	HashHDel
)

type Entry struct {

	// header size: 4 + 8 + 2 + 4 + 4 + 4 = 26 bytes
	crc       uint32
	timestamp uint64
	state     uint16 // high 8 bit is data type, low 8 bit is mark type
	keySize   uint32 // max key size is 3.99G
	valueSize uint32 // max value size is 3.99G
	keyOffset uint32 // the boundary between two keys

	// actual data
	key   []byte
	value []byte
}

func NewEntry(key, value []byte, dataType, markType uint16, keyOffset uint32) *Entry {
	e := &Entry{
		crc:       crc32.ChecksumIEEE(value),
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
		keyOffset: keyOffset,
		key:       key,
		value:     value,
	}
	e.state |= dataType << 8
	e.state |= markType
	e.timestamp = uint64(time.Now().UnixNano())

	return e
}

func (e *Entry) Size() uint32 {
	return EntryHeaderSize + e.keySize + e.valueSize
}

func (e *Entry) GetPreKey() string {
	var pre []byte
	copy(pre, e.key[:e.keyOffset])
	return string(pre)
}

func (e *Entry) GetPostKey() string {
	var post []byte
	copy(post, e.key[e.keyOffset:])
	return string(post)
}

func (e *Entry) GetPostBytesKey() []byte {
	var post []byte
	copy(post, e.key[e.keyOffset:])
	return post
}

func (e *Entry) GetDataType() uint16 {
	return e.state >> 8
}

func (e *Entry) GetMarkType() uint16 {
	return e.state & (2<<7 - 1)
}

// put entry in byte slice
func (e *Entry) Encode() ([]byte, error) {
	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[0:4], e.crc)
	binary.BigEndian.PutUint64(buf[4:12], e.timestamp)
	binary.BigEndian.PutUint16(buf[12:14], e.state)
	binary.BigEndian.PutUint32(buf[14:18], e.keySize)
	binary.BigEndian.PutUint32(buf[18:22], e.valueSize)
	binary.BigEndian.PutUint32(buf[22:26], e.keyOffset)

	st := uint32(EntryHeaderSize)
	ed := st + e.keySize
	copy(buf[st:ed], e.key)
	st = ed
	ed += e.valueSize
	copy(buf[st:ed], e.value)

	return buf, nil
}

// read entry header from buf
func DecodeHeader(buf []byte) (*Entry, error) {
	e := &Entry{}

	e.crc = binary.BigEndian.Uint32(buf[:4])
	e.timestamp = binary.BigEndian.Uint64(buf[4:12])
	e.state = binary.BigEndian.Uint16(buf[12:14])
	e.keySize = binary.BigEndian.Uint32(buf[14:18])
	e.valueSize = binary.BigEndian.Uint32(buf[18:22])
	e.keyOffset = binary.BigEndian.Uint32(buf[22:26])

	return e, nil
}
