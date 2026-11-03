package inner

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// primitivas para escribir y leer los campos de todos los mensajes

const maxStringSize = 64 * 1024 * 1024 // 64 MB

var ErrStringTooLarge = errors.New("inner: string too large")

// uint32 largo + bytes
func appendString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

// uint32 cantidad + cada string con su largo
func appendStrings(buf []byte, list []string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(list)))
	for _, s := range list {
		buf = appendString(buf, s)
	}
	return buf
}

func appendUint32(buf []byte, n uint32) []byte {
	return binary.BigEndian.AppendUint32(buf, n)
}

func appendUint64(buf []byte, n uint64) []byte {
	return binary.BigEndian.AppendUint64(buf, n)
}

func appendFloat64(buf []byte, f float64) []byte {
	return binary.BigEndian.AppendUint64(buf, math.Float64bits(f))
}

func readUint32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func readFloat64(r io.Reader) (float64, error) {
	bits, err := readUint64(r)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(bits), nil
}

func readString(r io.Reader) (string, error) {
	size, err := readUint32(r)
	if err != nil {
		return "", err
	}
	if size > maxStringSize {
		return "", ErrStringTooLarge
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", io.ErrUnexpectedEOF
	}
	return string(buf), nil
}

func readStrings(r *bytes.Reader) ([]string, error) {
	n, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	if int64(n)*4 > int64(r.Len()) { // cada string ocupa al menos 4 bytes
		return nil, io.ErrUnexpectedEOF
	}
	list := make([]string, 0, n)
	for range n {
		s, err := readString(r)
		if err != nil {
			return nil, err
		}
		list = append(list, s)
	}
	return list, nil
}
