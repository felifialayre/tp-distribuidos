package inner

import (
	"bytes"
	"io"
)

// filter/splitter -> image downloader

type Biography struct {
	Name     string
	URL      string
	ImageURL string
}

func (b *Biography) fields() []*string {
	return []*string{&b.Name, &b.URL, &b.ImageURL}
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
		if *field, err = readString(r); err != nil {
			return Header{}, Biography{}, err
		}
	}
	if r.Len() != 0 {
		return Header{}, Biography{}, io.ErrShortBuffer
	}
	return h, bio, nil
}
