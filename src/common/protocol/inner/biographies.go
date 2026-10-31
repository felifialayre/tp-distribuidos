package inner

import (
	"bytes"
	"io"

	"tp-distribuidos/common/parquet_row"
)

// splitter(filter) -> image downloader

type Biography struct {
	Name     string
	URL      string
	Sections string
	ImageURL string
}

func (b *Biography) fields() []*string {
	return []*string{&b.Name, &b.URL, &b.Sections, &b.ImageURL}
}

func EncodeBiography(h Header, bio Biography) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)), OpBiography, h)
	for _, field := range bio.fields() {
		msg = appendString(msg, *field)
	}
	return msg
}

func DecodeBiography(data []byte) (Header, Biography, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpBiography)
	if err != nil {
		return Header{}, Biography{}, err
	}
	var bio Biography
	for _, field := range bio.fields() {
		if *field, err = parquet_row.ReadString(r); err != nil {
			return Header{}, Biography{}, err
		}
	}
	if r.Len() != 0 {
		return Header{}, Biography{}, io.ErrShortBuffer
	}
	return h, bio, nil
}
