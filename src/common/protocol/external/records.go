package external

import (
	"encoding/binary"

	"tp-distribuidos/common/parquet_row"
)

// records: client -> gateway
// las filas/rows que le envia

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
	return p.readRow()
}

func (p *ExternalProtocol) readRow() (*parquet_row.ParquetRow, error) {
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

// Batch
//	uint32  opcode BATCH
//	uint32  cant filas/rows
//	bytes filas

func (p *ExternalProtocol) WriteBatch(rows []*parquet_row.ParquetRow) error {
	if len(rows) > MaxBatchRows {
		return ErrBatchTooLarge
	}

	msg := binary.BigEndian.AppendUint32(nil, uint32(Batch))
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(rows)))
	for _, row := range rows {
		body, err := row.Encode()
		if err != nil {
			return err
		}
		msg = append(msg, body...)
	}
	return p.socket.Send(msg)
}

// ReadBatch se llama despues de ReadMsgType cuando el tipo fue Batch
func (p *ExternalProtocol) ReadBatch() ([]*parquet_row.ParquetRow, error) {
	count, err := p.readUint32()
	if err != nil {
		return nil, err
	}
	if count > MaxBatchRows {
		return nil, ErrBatchTooLarge
	}

	rows := make([]*parquet_row.ParquetRow, 0, min(count, 1024)) //reservar memoria pero con limite
	for range count {
		row, err := p.readRow()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}
