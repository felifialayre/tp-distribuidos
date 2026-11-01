package inner

import (
	"bytes"
	"io"
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
	lang, err := readString(r)
	if err != nil {
		return Header{}, LangWords{}, err
	}
	if r.Len() != 0 {
		return Header{}, LangWords{}, io.ErrShortBuffer
	}
	return h, LangWords{Words: words, Lang: lang}, nil
}
