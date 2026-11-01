package external

import (
	"bytes"
	"encoding/binary"

	"tp-distribuidos/common/parquet_row"
)

// records: client -> gateway
// las filas/rows que le envia

func (p *ExternalProtocol) WriteEndOfRecords() error {
	return p.writeMsgType(EndOfRecords)
}

// Batch
//	uint32  opcode BATCH
//	uint32  size del resto de mensaje
//	contenido:
//		uint32  cant filas/rows
//		por cada row (parquet_row.Encode)

func (p *ExternalProtocol) WriteBatch(rows []*parquet_row.ParquetRow) error {
	if len(rows) > MaxBatchRows {
		return ErrBatchTooLarge
	}

	body := binary.BigEndian.AppendUint32(nil, uint32(len(rows)))
	for _, row := range rows {
		encoded, err := row.Encode()
		if err != nil {
			return err
		}
		body = append(body, encoded...)
	}
	if len(body) > MaxBatchSize {
		return ErrBatchTooLarge
	}

	msg := binary.BigEndian.AppendUint32(nil, uint32(Batch))
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(body)))
	msg = append(msg, body...)
	return p.socket.Send(msg)
}

// lee todo el batch del socket de una vez y despues arma las filas desde memoria
func (p *ExternalProtocol) ReadBatch() ([]*parquet_row.ParquetRow, error) {
	size, err := p.readUint32()
	if err != nil {
		return nil, err
	}
	if size > MaxBatchSize {
		return nil, ErrBatchTooLarge
	}

	buf := make([]byte, size)
	if err := p.socket.Receive(buf); err != nil {
		return nil, err
	}
	if len(buf) < uint32Size {
		return nil, ErrBadBatch
	}

	count := binary.BigEndian.Uint32(buf[:uint32Size])
	if count > MaxBatchRows {
		return nil, ErrBatchTooLarge
	}

	reader := bytes.NewReader(buf[uint32Size:])
	rows := make([]*parquet_row.ParquetRow, 0, count)
	for range count {
		row, err := parquet_row.Decode(reader)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if reader.Len() != 0 {
		return nil, ErrBadBatch // sobraron bytes: el size no coincide con las filas
	}
	return rows, nil
}
