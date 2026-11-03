package inner

import (
	"bytes"
	"io"
)

type WordCount struct {
	Word  string
	Count uint32
}

// stopwords -> accumulator
// las palabras de un abstract y su fecha (el accumulator la usa para el promedio de Q5)
type Words struct {
	Words        []string
	DateModified string
}

func EncodeWords(h Header, w Words) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)), OpWords, h)
	msg = appendStrings(msg, w.Words)
	return appendString(msg, w.DateModified)
}

func DecodeWords(data []byte) (Header, Words, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpWords)
	if err != nil {
		return Header{}, Words{}, err
	}
	words, err := readStrings(r)
	if err != nil {
		return Header{}, Words{}, err
	}
	date, err := readString(r)
	if err != nil {
		return Header{}, Words{}, err
	}
	if r.Len() != 0 {
		return Header{}, Words{}, io.ErrShortBuffer
	}
	return h, Words{Words: words, DateModified: date}, nil
}

func EncodeWordCounts(h Header, counts []WordCount) []byte {
	return encodeCounts(OpWordCounts, h, counts)
}

func DecodeWordCounts(data []byte) (Header, []WordCount, error) {
	return decodeCounts(OpWordCounts, data)
}

func EncodeQ4Result(h Header, counts []WordCount) []byte {
	return encodeCounts(OpQ4Result, h, counts)
}

func DecodeQ4Result(data []byte) (Header, []WordCount, error) {
	return decodeCounts(OpQ4Result, data)
}

func encodeCounts(op Opcode, h Header, counts []WordCount) []byte {
	msg := appendHeader(make([]byte, 0, headerSize(h)+4), op, h)
	msg = appendUint32(msg, uint32(len(counts)))
	for _, c := range counts {
		msg = appendString(msg, c.Word)
		msg = appendUint32(msg, c.Count)
	}
	return msg
}

func decodeCounts(op Opcode, data []byte) (Header, []WordCount, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, op)
	if err != nil {
		return Header{}, nil, err
	}
	n, err := readUint32(r)
	if err != nil {
		return Header{}, nil, err
	}
	if int64(n)*8 > int64(r.Len()) {
		return Header{}, nil, io.ErrUnexpectedEOF
	}
	counts := make([]WordCount, 0, n)
	for range n {
		word, err := readString(r)
		if err != nil {
			return Header{}, nil, err
		}
		count, err := readUint32(r)
		if err != nil {
			return Header{}, nil, err
		}
		counts = append(counts, WordCount{Word: word, Count: count})
	}
	if r.Len() != 0 {
		return Header{}, nil, io.ErrShortBuffer
	}
	return h, counts, nil
}
