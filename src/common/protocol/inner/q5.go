package inner

import (
	"bytes"
	"io"
)

// Q5Article es un articulo de 2021 en adelante con su cantidad de palabras
// stopwords -> above average filter, y above average filter -> gateway (Q5Result)
type Q5Article struct {
	URL   string
	Count uint32
}

func EncodeQ5Article(h Header, a Q5Article) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)+4+len(a.URL)+4), OpQ5Article, h)
	msg = appendString(msg, a.URL)
	return appendUint32(msg, a.Count)
}

func DecodeQ5Article(data []byte) (Header, Q5Article, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpQ5Article)
	if err != nil {
		return Header{}, Q5Article{}, err
	}
	a, err := readQ5Article(r)
	if err != nil {
		return Header{}, Q5Article{}, err
	}
	if r.Len() != 0 {
		return Header{}, Q5Article{}, io.ErrShortBuffer
	}
	return h, a, nil
}

func EncodeQ5Result(h Header, articles []Q5Article) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)+4), OpQ5Result, h)
	msg = appendUint32(msg, uint32(len(articles)))
	for _, a := range articles {
		msg = appendString(msg, a.URL)
		msg = appendUint32(msg, a.Count)
	}
	return msg
}

func DecodeQ5Result(data []byte) (Header, []Q5Article, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpQ5Result)
	if err != nil {
		return Header{}, nil, err
	}
	n, err := readUint32(r)
	if err != nil {
		return Header{}, nil, err
	}
	if int64(n)*8 > int64(r.Len()) { // cada articulo ocupa al menos 8 bytes
		return Header{}, nil, io.ErrUnexpectedEOF
	}
	articles := make([]Q5Article, 0, n)
	for range n {
		a, err := readQ5Article(r)
		if err != nil {
			return Header{}, nil, err
		}
		articles = append(articles, a)
	}
	if r.Len() != 0 {
		return Header{}, nil, io.ErrShortBuffer
	}
	return h, articles, nil
}

func readQ5Article(r *bytes.Reader) (Q5Article, error) {
	url, err := readString(r)
	if err != nil {
		return Q5Article{}, err
	}
	count, err := readUint32(r)
	if err != nil {
		return Q5Article{}, err
	}
	return Q5Article{URL: url, Count: count}, nil
}

// PartialAverage es el promedio parcial de un accumulator, guardado como suma y cantidad
// para poder juntar los de todos: palabras y abstracts anteriores a 2021.
// accumulator -> join average
type PartialAverage struct {
	Words     uint64
	Abstracts uint64
}

func EncodePartialAverage(h Header, p PartialAverage) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)+16), OpPartialAverage, h)
	msg = appendUint64(msg, p.Words)
	return appendUint64(msg, p.Abstracts)
}

func DecodePartialAverage(data []byte) (Header, PartialAverage, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpPartialAverage)
	if err != nil {
		return Header{}, PartialAverage{}, err
	}
	words, err := readUint64(r)
	if err != nil {
		return Header{}, PartialAverage{}, err
	}
	abstracts, err := readUint64(r)
	if err != nil {
		return Header{}, PartialAverage{}, err
	}
	if r.Len() != 0 {
		return Header{}, PartialAverage{}, io.ErrShortBuffer
	}
	return h, PartialAverage{Words: words, Abstracts: abstracts}, nil
}

// Average es el promedio de palabras de los abstracts anteriores a 2021
// join average -> above average filter
type Average struct {
	Value float64
}

func EncodeAverage(h Header, avg Average) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)+8), OpAverage, h)
	return appendFloat64(msg, avg.Value)
}

func DecodeAverage(data []byte) (Header, Average, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpAverage)
	if err != nil {
		return Header{}, Average{}, err
	}
	value, err := readFloat64(r)
	if err != nil {
		return Header{}, Average{}, err
	}
	if r.Len() != 0 {
		return Header{}, Average{}, io.ErrShortBuffer
	}
	return h, Average{Value: value}, nil
}
