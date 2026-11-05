package external

import (
	"bytes"
	"encoding/binary"
	"io"
	"strconv"

	"tp-distribuidos/common/parquet_row"
)

// results: gateway -> cliente
// los resultados de cada consulta, en batches. Un batch tiene filas de una sola consulta
//
// ResultBatch
//	uint32  opcode ResultBatch
//	uint32  query
//	uint32  size del resto del mensaje
//	contenido:
//		uint32  cantidad de filas
//		por cada fila:
//			uint32  cantidad de campos
//			        por cada campo: uint32 largo + bytes
//
// EndOfResults avisa que no hay mas resultados de una consulta
//	uint32  opcode EndOfResults
//	uint32  query

// las filas de cada consulta, con sus campos en el orden en que van al csv

// una fila de la salida de Q1
func Q1Row(url, dateModified string) []string {
	return []string{url, dateModified}
}

// una fila de la salida de Q2: nombre del articulo, su url y los bytes de la
// imagen (puede venir vacia si el articulo no tenia imagen setteada)
func Q2Row(name, url string, imageBytes []byte) []string {
	return []string{name, url, string(imageBytes)}
}

// una fila de la salida de Q3
func Q3Row(word string) []string {
	return []string{word}
}

// una fila de la salida de Q4, la palabra y en cuantos abstracts aparece
func Q4Row(word string, count uint32) []string {
	return []string{word, strconv.FormatUint(uint64(count), 10)}
}

// una fila de la salida de Q5, la url y la cantidad de palabras de su abstract
func Q5Row(url string, count uint32) []string {
	return []string{url, strconv.FormatUint(uint64(count), 10)}
}

// ResultBuffer junta filas de una consulta, ya codificadas, hasta que se mandan en un ResultBatch
// lo usa el gateway para decidir cuando un batch esta lleno mirando su tamano en bytes
type ResultBuffer struct {
	query Query
	rows  uint32
	body  []byte // las filas codificadas, una atras de otra
}

func NewResultBuffer(query Query) *ResultBuffer {
	return &ResultBuffer{query: query}
}

// RowSize es lo que ocupa una fila una vez codificada
func RowSize(fields []string) int {
	size := uint32Size
	for _, field := range fields {
		size += uint32Size + len(field)
	}
	return size
}

// Add agrega una fila, si falla no modifica el buffer
func (b *ResultBuffer) Add(fields []string) error {
	if len(fields) > maxResultFields {
		return ErrResultTooLarge
	}
	for _, field := range fields {
		if len(field) > parquet_row.MaxFieldSize {
			return ErrFieldTooLarge
		}
	}
	if uint32Size+len(b.body)+RowSize(fields) > MaxResultBatchSize {
		return ErrBatchTooLarge
	}

	b.body = binary.BigEndian.AppendUint32(b.body, uint32(len(fields)))
	for _, field := range fields {
		b.body = binary.BigEndian.AppendUint32(b.body, uint32(len(field)))
		b.body = append(b.body, field...)
	}
	b.rows++
	return nil
}

// Size son los bytes de las filas que hay en el buffer
func (b *ResultBuffer) Size() int {
	return len(b.body)
}

// Len es la cantidad de filas que hay en el buffer
func (b *ResultBuffer) Len() int {
	return int(b.rows)
}

// Reset vacia el buffer
func (b *ResultBuffer) Reset() {
	b.rows = 0
	b.body = nil // no se reusa el slice, un batch con una fila enorme no tiene que dejar memoria retenida
}

func (p *ExternalProtocol) WriteResultBatch(b *ResultBuffer) error {
	size := uint32Size + len(b.body) // cantidad de filas + las filas

	msg := make([]byte, 0, 3*uint32Size+size)
	msg = binary.BigEndian.AppendUint32(msg, uint32(ResultBatch))
	msg = binary.BigEndian.AppendUint32(msg, uint32(b.query))
	msg = binary.BigEndian.AppendUint32(msg, uint32(size))
	msg = binary.BigEndian.AppendUint32(msg, b.rows)
	msg = append(msg, b.body...)
	return p.socket.Send(msg)
}

// lee todo el batch del socket de una vez y despues arma las filas desde memoria
func (p *ExternalProtocol) ReadResultBatch() (Query, [][]string, error) {
	query, err := p.readUint32()
	if err != nil {
		return 0, nil, err
	}
	size, err := p.readUint32()
	if err != nil {
		return 0, nil, err
	}
	if size > MaxResultBatchSize {
		return 0, nil, ErrBatchTooLarge
	}

	buf := make([]byte, size)
	if err := p.socket.Receive(buf); err != nil {
		return 0, nil, err
	}
	if len(buf) < uint32Size {
		return 0, nil, ErrBadBatch
	}

	count := binary.BigEndian.Uint32(buf[:uint32Size])
	reader := bytes.NewReader(buf[uint32Size:])
	if int64(count)*uint32Size > int64(reader.Len()) { // cada fila ocupa al menos 4 bytes
		return 0, nil, ErrBadBatch
	}

	rows := make([][]string, 0, count)
	for range count {
		fields, err := readRow(reader)
		if err != nil {
			return 0, nil, err
		}
		rows = append(rows, fields)
	}
	if reader.Len() != 0 {
		return 0, nil, ErrBadBatch // sobraron bytes, el size no coincide con las filas
	}
	return Query(query), rows, nil
}

func readRow(reader *bytes.Reader) ([]string, error) {
	count, err := readUint32From(reader)
	if err != nil {
		return nil, err
	}
	if count > maxResultFields {
		return nil, ErrResultTooLarge
	}

	fields := make([]string, 0, count)
	for range count {
		size, err := readUint32From(reader)
		if err != nil {
			return nil, err
		}
		if size > parquet_row.MaxFieldSize {
			return nil, ErrFieldTooLarge
		}
		if int64(size) > int64(reader.Len()) {
			return nil, ErrBadBatch
		}
		field := make([]byte, size)
		if _, err := io.ReadFull(reader, field); err != nil {
			return nil, ErrBadBatch
		}
		fields = append(fields, string(field))
	}
	return fields, nil
}

func readUint32From(reader io.Reader) (uint32, error) {
	var buf [uint32Size]byte
	if _, err := io.ReadFull(reader, buf[:]); err != nil {
		return 0, ErrBadBatch
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

func (p *ExternalProtocol) WriteEndOfResults(query Query) error {
	msg := binary.BigEndian.AppendUint32(nil, uint32(EndOfResults))
	msg = binary.BigEndian.AppendUint32(msg, uint32(query))
	return p.socket.Send(msg)
}

func (p *ExternalProtocol) ReadEndOfResults() (Query, error) {
	query, err := p.readUint32()
	if err != nil {
		return 0, err
	}
	return Query(query), nil
}
