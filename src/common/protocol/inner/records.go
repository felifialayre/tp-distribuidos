package inner

import (
	"bytes"
	"io"
)

// gateway -> filters (workers)

// son las columnas necesarias para las queries, ya sin el formato del parquet
type Record struct {
	Name         string
	URL          string
	InLanguage   string
	DateCreated  string
	DateModified string
	SectionNames []string
	ImageURL     string
	Abstract     string
}

func (r *Record) fields() []*string {
	return []*string{&r.Name, &r.URL, &r.InLanguage, &r.DateCreated, &r.DateModified, &r.ImageURL, &r.Abstract}
}

func EncodeRecord(h Header, rec Record) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)), OpRecord, h)
	for _, field := range rec.fields() {
		msg = appendString(msg, *field)
	}
	return appendStrings(msg, rec.SectionNames)
}

func DecodeRecord(data []byte) (Header, Record, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpRecord)
	if err != nil {
		return Header{}, Record{}, err
	}
	var rec Record
	for _, field := range rec.fields() {
		if *field, err = readString(r); err != nil {
			return Header{}, Record{}, err
		}
	}
	if rec.SectionNames, err = readStrings(r); err != nil {
		return Header{}, Record{}, err
	}
	if r.Len() != 0 {
		return Header{}, Record{}, io.ErrShortBuffer
	}
	return h, rec, nil
}
