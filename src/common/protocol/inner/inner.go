package inner

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"tp-distribuidos/common/parquet_row"
)

// tipo de mensaje
type Opcode uint8

const (
	OpRecord Opcode = iota + 1
	OpEndOfRecords
	OpQ1Result
	OpEndOfResults
)

// numero de query a la que pertenece el resultado
type Query uint8

const (
	Q1 Query = iota + 1
)

var ErrUnexpectedOpcode = errors.New("inner: unexpected opcode")

type Header struct {
	ClientID  string
	GatewayID string
}

// Formato mensajes:
//		uint8 opcode
//		uint32 largo + ClientID
//		uint32 largo +  GatewayID
//		body (encode/decode)

func ReadOpcode(data []byte) (Opcode, error) {
	if len(data) == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	return Opcode(data[0]), nil
}

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

// Q1Result es una fila de la salida de Q1
type Q1Result struct {
	URL          string
	DateModified string
}

// EncodeQ1Result serializa (payload: URL y DateModified con su largo)
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
	url, err := parquet_row.ReadString(r)
	if err != nil {
		return Header{}, Q1Result{}, err
	}
	date, err := parquet_row.ReadString(r)
	if err != nil {
		return Header{}, Q1Result{}, err
	}
	if r.Len() != 0 {
		return Header{}, Q1Result{}, io.ErrShortBuffer
	}
	return h, Q1Result{URL: url, DateModified: date}, nil
}

// EncodeEndOfResults avisa que no hay mas resultados de esa query para el
// cliente del Header (payload: 1 byte con la query)
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

func headerSize(h Header) int {
	return 1 + 2*4 + len(h.ClientID) + len(h.GatewayID)
}

func appendHeader(buf []byte, op Opcode, h Header) []byte {
	buf = append(buf, byte(op))
	buf = appendString(buf, h.ClientID)
	return appendString(buf, h.GatewayID)
}

// readHeader lee el opcode y verifica que sea el esperado antes de leer el Header
func readHeader(r *bytes.Reader, expected Opcode) (Header, error) {
	op, err := r.ReadByte()
	if err != nil {
		return Header{}, io.ErrUnexpectedEOF
	}
	if Opcode(op) != expected {
		return Header{}, ErrUnexpectedOpcode
	}

	clientID, err := parquet_row.ReadString(r)
	if err != nil {
		return Header{}, err
	}
	gatewayID, err := parquet_row.ReadString(r)
	if err != nil {
		return Header{}, err
	}
	return Header{ClientID: clientID, GatewayID: gatewayID}, nil
}

func appendString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}
