package inner

import (
	"bytes"
	"io"

	"tp-distribuidos/common/parquet_row"
)

// gateway -> filters (workers)

// EncodeRecord serializa el Header y la row
func EncodeRecord(h Header, row *parquet_row.ParquetRow) ([]byte, error) {
	body, err := row.Encode()
	if err != nil {
		return nil, err
	}

	msg := appendHeader(make([]byte, 0, headerSize(h)+len(body)), OpRecord, h)
	return append(msg, body...), nil
}

func DecodeRecord(data []byte) (Header, *parquet_row.ParquetRow, error) {
	r := bytes.NewReader(data)

	h, err := readHeader(r, OpRecord)
	if err != nil {
		return Header{}, nil, err
	}
	row, err := parquet_row.Decode(r)
	if err != nil {
		return Header{}, nil, err
	}
	if r.Len() != 0 {
		return Header{}, nil, io.ErrShortBuffer
	}
	return h, row, nil
}

// EncodeEndOfRecords avisa que el cliente del Header no manda mas filas (sin payload)
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
