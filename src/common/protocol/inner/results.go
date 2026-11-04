package inner

import (
	"bytes"
	"io"
)

// workers -> gateway
// los resultados de cada query

// numero de query a la que pertenece el resultado
type Query uint8

const (
	Q1 Query = iota + 1
	Q2
	Q3
	Q4
	Q5
)

// Q1Result es una fila de la salida de Q1
type Q1Result struct {
	URL          string
	DateModified string
}

func EncodeQ1Result(h Header, res Q1Result) []byte {
	size := headerSize(h) + 2*4 + len(res.URL) + len(res.DateModified)
	msg := appendHeader(make([]byte, 0, size), OpQ1Result, h)
	msg = appendString(msg, res.URL)
	return appendString(msg, res.DateModified)
}

func DecodeQ1Result(data []byte) (Header, Q1Result, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpQ1Result)
	if err != nil {
		return Header{}, Q1Result{}, err
	}
	url, err := readString(r)
	if err != nil {
		return Header{}, Q1Result{}, err
	}
	date, err := readString(r)
	if err != nil {
		return Header{}, Q1Result{}, err
	}
	if r.Len() != 0 {
		return Header{}, Q1Result{}, io.ErrShortBuffer
	}
	return h, Q1Result{URL: url, DateModified: date}, nil
}

// Q2Result es una biografia con su imagen ya descargada (puede venir vacia si el
// articulo no tenia imagen)

// Q2Result viene con el título del artículo, su URL y los bytes de la imágen
// si la URL venía vacía, los bytes de la imágen también lo harán.
type Q2Result struct {
	Name       string
	URL        string
	ImageBytes []byte
}

func EncodeQ2Result(h Header, res Q2Result) []byte {
	size := headerSize(h) + 3*4 + len(res.Name) + len(res.URL) + len(res.ImageBytes)
	msg := appendHeader(make([]byte, 0, size), OpQ2Result, h)
	msg = appendString(msg, res.Name)
	msg = appendString(msg, res.URL)
	return appendString(msg, string(res.ImageBytes))
}

func DecodeQ2Result(data []byte) (Header, Q2Result, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpQ2Result)
	if err != nil {
		return Header{}, Q2Result{}, err
	}
	name, err := readString(r)
	if err != nil {
		return Header{}, Q2Result{}, err
	}
	url, err := readString(r)
	if err != nil {
		return Header{}, Q2Result{}, err
	}
	image, err := readString(r)
	if err != nil {
		return Header{}, Q2Result{}, err
	}
	if r.Len() != 0 {
		return Header{}, Q2Result{}, io.ErrShortBuffer
	}
	return h, Q2Result{Name: name, URL: url, ImageBytes: []byte(image)}, nil
}

// Q3Result son palabras que ya aparecieron en articulos en ingles y en frances
type Q3Result struct {
	Words []string
}

func EncodeQ3Result(h Header, res Q3Result) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)), OpQ3Result, h)
	return appendStrings(msg, res.Words)
}

func DecodeQ3Result(data []byte) (Header, Q3Result, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpQ3Result)
	if err != nil {
		return Header{}, Q3Result{}, err
	}
	words, err := readStrings(r)
	if err != nil {
		return Header{}, Q3Result{}, err
	}
	if r.Len() != 0 {
		return Header{}, Q3Result{}, io.ErrShortBuffer
	}
	return h, Q3Result{Words: words}, nil
}

func EncodeEndOfResults(h Header, q Query) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)+1), OpEndOfResults, h)
	return append(msg, byte(q))
}

func DecodeEndOfResults(data []byte) (Header, Query, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpEndOfResults)
	if err != nil {
		return Header{}, 0, err
	}
	q, err := r.ReadByte()
	if err != nil {
		return Header{}, 0, io.ErrUnexpectedEOF
	}
	if r.Len() != 0 {
		return Header{}, 0, io.ErrShortBuffer
	}
	return h, Query(q), nil
}
