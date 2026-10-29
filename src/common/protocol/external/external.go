package external

import (
	"encoding/binary"

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
)

var ErrFieldTooLarge = parquet_row.ErrFieldTooLarge

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

func (p *ExternalProtocol) WriteRecord(row *parquet_row.ParquetRow) error {
	body, err := row.Encode()
	if err != nil {
		return err
	}

	msg := make([]byte, 0, uint32Size+len(body))
	msg = binary.BigEndian.AppendUint32(msg, uint32(Record))
	msg = append(msg, body...)
	return p.socket.Send(msg)
}

func (p *ExternalProtocol) ReadRecord() (*parquet_row.ParquetRow, error) {
	identifier, err := p.readUint64()
	if err != nil {
		return nil, err
	}

	row := &parquet_row.ParquetRow{Identifier: int64(identifier)}
	for _, field := range row.StringFields() {
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
	if size > parquet_row.MaxFieldSize {
		return "", ErrFieldTooLarge
	}

	buf := make([]byte, size)
	if err := p.socket.Receive(buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
