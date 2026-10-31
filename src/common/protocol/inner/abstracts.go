package inner

import (
	"bytes"
	"io"

	"tp-distribuidos/common/parquet_row"
)

// splitter -> filter de stopwords

// Abstract es lo que necesitan Q3, Q4 y Q5 de cada articulo
type Abstract struct {
	URL          string
	Lang         string
	DateModified string
	Text         string
}

func (a *Abstract) fields() []*string {
	return []*string{&a.URL, &a.Lang, &a.DateModified, &a.Text}
}

func EncodeAbstract(h Header, abs Abstract) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)), OpAbstract, h)
	for _, field := range abs.fields() {
		msg = appendString(msg, *field)
	}
	return msg
}

func DecodeAbstract(data []byte) (Header, Abstract, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpAbstract)
	if err != nil {
		return Header{}, Abstract{}, err
	}
	var abs Abstract
	for _, field := range abs.fields() {
		if *field, err = parquet_row.ReadString(r); err != nil {
			return Header{}, Abstract{}, err
		}
	}
	if r.Len() != 0 {
		return Header{}, Abstract{}, io.ErrShortBuffer
	}
	return h, abs, nil
}
