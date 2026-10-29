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
	Batch
	Result
	EndOfResults
)

// Query identifica a que consulta pertenece un resultado
type Query uint32

const Q1 Query = iota + 1

const (
	uint32Size      = 4
	uint64Size      = 8
	MaxBatchRows    = 100_000
	maxResultFields = 64
)

var (
	ErrFieldTooLarge  = parquet_row.ErrFieldTooLarge
	ErrBatchTooLarge  = errors.New("external: batch too large")
	ErrResultTooLarge = errors.New("external: too many result fields")
)

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

func (p *ExternalProtocol) RecvACK() error {
	msgType, err := p.ReadMsgType()
	if err != nil {
		return err
	}
	if msgType != Ack {
		return errors.New("unexpected message type")
	}
	return nil
}

func (p *ExternalProtocol) Close() error {
	return p.socket.Close()
}
