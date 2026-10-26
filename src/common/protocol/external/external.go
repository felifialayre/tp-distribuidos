package external

import (
	"encoding/binary"
	"errors"

	"tp-distribuidos/common/parquet_row"
	"tp-distribuidos/common/socket"
)

type MsgType uint32

const (
	Ack MsgType = iota
	EndOfRecords
	Record
)

const (
	uint32Size = 4
	uint64Size = 8

	maxFieldSize = 64 * 1024 * 1024 // 64 MB
)

var ErrFieldTooLarge = errors.New("external: field too large")

type ExternalProtocol struct {
	socket *socket.Socket
}

func NewExternalProtocol(s *socket.Socket) *ExternalProtocol {
	return &ExternalProtocol{socket: s}
}

func (p *ExternalProtocol) writeMsgType(msgType MsgType) error {
	msg := binary.BigEndian.AppendUint32(nil, uint32(msgType))
	return p.socket.Send(msg)
}

func (p *ExternalProtocol) ReadMsgType() (MsgType, error) {
	value, err := p.readUint32()
	if err != nil {
		return 0, err
	}
	return MsgType(value), nil
}

func (p *ExternalProtocol) WriteAck() error {
	return p.writeMsgType(Ack)
}

func (p *ExternalProtocol) WriteEndOfRecords() error {
	return p.writeMsgType(EndOfRecords)
}

// ---------------------------------------------------------------------
// Record (una fila del parquet)
//
// Formato en el cable (todo big endian). La cantidad de columnas es fija
// (ParquetRow), asi que no se manda:
//
//	uint32  tipo de mensaje (Record)
//	uint64  identifier
//	por cada columna de texto, en el orden de stringFields:
//	    uint32  largo en bytes
//	    bytes   contenido
// ---------------------------------------------------------------------

// stringFields devuelve las columnas de texto de la fila, en el orden en que
// viajan por el cable. WriteRecord y ReadRecord usan la misma lista, asi el
// orden no puede quedar desincronizado entre uno y otro.
func stringFields(row *parquet_row.ParquetRow) []*string {
	return []*string{
		&row.Abstract,
		&row.AdditionalEntities,
		&row.DateCreated,
		&row.DateModified,
		&row.Description,
		&row.Event,
		&row.Image,
		&row.InLanguage,
		&row.Infoboxes,
		&row.IsPartOf,
		&row.License,
		&row.MainEntity,
		&row.Name,
		&row.References,
		&row.Sections,
		&row.Tables,
		&row.URL,
		&row.Version,
	}
}

// WriteRecord arma el mensaje completo en memoria y lo manda con un solo Send
func (p *ExternalProtocol) WriteRecord(row *parquet_row.ParquetRow) error {
	fields := stringFields(row)

	size := uint32Size + uint64Size
	for _, field := range fields {
		if len(*field) > maxFieldSize {
			return ErrFieldTooLarge
		}
		size += uint32Size + len(*field)
	}

	msg := make([]byte, 0, size)
	msg = binary.BigEndian.AppendUint32(msg, uint32(Record))
	msg = binary.BigEndian.AppendUint64(msg, uint64(row.Identifier))
	for _, field := range fields {
		msg = binary.BigEndian.AppendUint32(msg, uint32(len(*field)))
		msg = append(msg, *field...)
	}

	return p.socket.Send(msg)
}

// ReadRecord se llama despues de ReadMsgType cuando el tipo fue Record
func (p *ExternalProtocol) ReadRecord() (*parquet_row.ParquetRow, error) {
	identifier, err := p.readUint64()
	if err != nil {
		return nil, err
	}

	row := &parquet_row.ParquetRow{Identifier: int64(identifier)}
	for _, field := range stringFields(row) {
		*field, err = p.readString()
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

func (p *ExternalProtocol) readUint32() (uint32, error) {
	var buf [uint32Size]byte
	if err := p.socket.Receive(buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

func (p *ExternalProtocol) readUint64() (uint64, error) {
	var buf [uint64Size]byte
	if err := p.socket.Receive(buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func (p *ExternalProtocol) readString() (string, error) {
	size, err := p.readUint32()
	if err != nil {
		return "", err
	}
	if size > maxFieldSize {
		return "", ErrFieldTooLarge
	}

	buf := make([]byte, size)
	if err := p.socket.Receive(buf); err != nil {
		return "", err
	}
	return string(buf), nil
}