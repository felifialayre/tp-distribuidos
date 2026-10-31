package inner

import (
	"bytes"
	"encoding/binary"
	"io"

	"tp-distribuidos/common/parquet_row"
)

// stopwords filter -> join

type LangWords struct {
	Words []string
	Lang  string
}

func EncodeLangWords(h Header, lw LangWords) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)), OpLangWords, h)
	msg = appendStrings(msg, lw.Words)
	return appendString(msg, lw.Lang)
}

func DecodeLangWords(data []byte) (Header, LangWords, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpLangWords)
	if err != nil {
		return Header{}, LangWords{}, err
	}
	words, err := readStrings(r)
	if err != nil {
		return Header{}, LangWords{}, err
	}
	lang, err := parquet_row.ReadString(r)
	if err != nil {
		return Header{}, LangWords{}, err
	}
	if r.Len() != 0 {
		return Header{}, LangWords{}, io.ErrShortBuffer
	}
	return h, LangWords{Words: words, Lang: lang}, nil
}

// uint32 cantidad + cada string con su largo
func appendStrings(buf []byte, list []string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(list)))
	for _, s := range list {
		buf = appendString(buf, s)
	}
	return buf
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
		s, err := parquet_row.ReadString(r)
		if err != nil {
			return nil, err
		}
		list = append(list, s)
	}
	return list, nil
}

func readUint32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}
