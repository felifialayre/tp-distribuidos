package external

import (
	"encoding/binary"
	"errors"

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

	maxFields = 1024
	maxFieldSize = 64 * 1024 * 1024 // 64 MB
)

var (
	ErrTooManyFields = errors.New("external: too many columns")
	ErrFieldTooLarge = errors.New("external: field too large")
)

type Row []string

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

// WriteRecord arma el mensaje completo en memoria y lo manda con un solo Send
func (p *ExternalProtocol) WriteRecord(row Row) error {
	if len(row) > maxFields {
		return ErrTooManyFields
	}

	size := 2 * uint32Size
	for _, field := range row {
		if len(field) > maxFieldSize {
			return ErrFieldTooLarge
		}
		size += uint32Size + len(field)
	}

	msg := make([]byte, 0, size)
	msg = binary.BigEndian.AppendUint32(msg, uint32(Record))
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(row)))
	for _, field := range row {
		msg = binary.BigEndian.AppendUint32(msg, uint32(len(field)))
		msg = append(msg, field...)
	}

	return p.socket.Send(msg)
}

// ReadRecord se llama despues de ReadMsgType cuando el tipo fue Record
func (p *ExternalProtocol) ReadRecord() (Row, error) {
	fieldCount, err := p.readUint32()
	if err != nil {
		return nil, err
	}
	if fieldCount > maxFields {
		return nil, ErrTooManyFields
	}

	row := make(Row, fieldCount)
	for i := range row {
		row[i], err = p.readString()
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

func (p *ExternalProtocol) readUint32() (uint32, error) {
	buf, err := p.socket.Receive(uint32Size)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf), nil
}
 
func (p *ExternalProtocol) readString() (string, error) {
	size, err := p.readUint32()
	if err != nil {
		return "", err
	}
	if size > maxFieldSize {
		return "", ErrFieldTooLarge
	}
 
	buf, err := p.socket.Receive(size)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}
 