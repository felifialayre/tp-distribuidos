package inner

import (
	"bytes"
	"io"

	"tp-distribuidos/common/parquet_row"
)

// gateway -> filters (workers)

// Record son las columnas necesarias para las queries
type Record struct {
	Name         string
	URL          string
	InLanguage   string
	DateCreated  string
	DateModified string
	Sections     string
	Image        string
	Abstract     string
}

func (r *Record) fields() []*string {
	return []*string{&r.Name, &r.URL, &r.InLanguage, &r.DateCreated, &r.DateModified, &r.Sections, &r.Image, &r.Abstract}
}

func EncodeRecord(h Header, rec Record) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)), OpRecord, h)
	for _, field := range rec.fields() {
		msg = appendString(msg, *field)
	}
	return msg
}

func DecodeRecord(data []byte) (Header, Record, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpRecord)
	if err != nil {
		return Header{}, Record{}, err
	}
	var rec Record
	for _, field := range rec.fields() {
		if *field, err = parquet_row.ReadString(r); err != nil {
			return Header{}, Record{}, err
		}
	}
	if r.Len() != 0 {
		return Header{}, Record{}, io.ErrShortBuffer
	}
	return h, rec, nil
}

func EncodeEndOfRecords(h Header) []byte {
	return appendHeader(make([]byte, 0, headerSize(h)), OpEndOfRecords, h)
}

func DecodeEndOfRecords(data []byte) (Header, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpEndOfRecords)
	if err != nil {
		return Header{}, err
	}
	if r.Len() != 0 {
		return Header{}, io.ErrShortBuffer
	}
	return h, nil
}
