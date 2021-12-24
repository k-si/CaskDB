package util

import (
	"bytes"
	"encoding/binary"
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
	x := int32(n)
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, x)
	return buf.Bytes()
}

func BytesToInt(b []byte) int {
	buf := bytes.NewBuffer(b)
	var x int32
	binary.Read(buf, binary.BigEndian, &x)
	return int(x)
}
