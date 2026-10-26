package serializer

import (
	"encoding/binary"
)

const UINT32_SIZE uint32 = 4
const BOOL_SIZE uint32 = 1

func appendLength(data []byte) []byte {
	length := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	return append(length, data...)
}

func SerializeString(value string) []byte {
	data := []byte(value)
	return appendLength(data)
}

func DeserializeString(b []byte) string {
	return string(b[:])
}

func SerializeUint32(value uint32) []byte {
	b := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(b, value)
	return b
}

func DeserializeUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}
