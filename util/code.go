package util

import (
	"bytes"
	"encoding/binary"
	"math"
)

func Encode(key, value interface{}) ([]byte, []byte, error) {
	return nil, nil, nil
}

func EncodeKey(key interface{}) ([]byte, error) {
	return nil, nil
}

func DecodeValue(value []byte) (interface{}, error) {
	return nil, nil
}

func IntToBytes(n int) []byte {
	x := int64(n)
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, x)
	return buf.Bytes()
}

func BytesToInt(b []byte) int {
	buf := bytes.NewBuffer(b)
	var x int64
	binary.Read(buf, binary.BigEndian, &x)
	return int(x)
}

func Float64ToBytes(n float64) []byte {
	bits := math.Float64bits(n)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

func BytesToFloat64(buf []byte) float64 {
	bits := binary.BigEndian.Uint64(buf)
	return math.Float64frombits(bits)
}
