package Adele

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const EntryHeaderSize = 22

// data type
const (
	String = iota
)

// mark type
const (
	StringSet = iota
	StringRemove
)

type Entry struct {

	// header size: 4 + 8 + 2 + 4 + 4 = 22 bytes
	crc       uint32
	timestamp uint64
	state     uint16 // high 8 bit is data type, low 8 bit is mark type
	keySize   uint32
	valueSize uint32

	// actual data
	key   []byte
	value []byte
}

func NewEntry(key, value []byte, dataType, markType uint16) *Entry {
	e := &Entry{
		crc:       crc32.ChecksumIEEE(value),
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
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

	return e, nil
}
