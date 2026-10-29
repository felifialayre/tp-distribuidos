package inner

import (
	"bytes"
	"encoding/binary"
	"io"

	"tp-distribuidos/common/parquet_row"
)

// Para cuando el mensaje vuelva
type Header struct {
	ClientID  string
	GatewayID string
}

// Serializa el Header y la row
// no hace falta delimitar porque Rabbit lo maneja
func EncodeRecord(h Header, row *parquet_row.ParquetRow) ([]byte, error) {
	body, err := row.Encode()
	if err != nil {
		return nil, err
	}

	msg := make([]byte, 0, 2*4+len(h.ClientID)+len(h.GatewayID)+len(body))
	msg = appendString(msg, h.ClientID)
	msg = appendString(msg, h.GatewayID)
	return append(msg, body...), nil
}

func DecodeRecord(data []byte) (Header, *parquet_row.ParquetRow, error) {
	r := bytes.NewReader(data)

	clientID, err := parquet_row.ReadString(r)
	if err != nil {
		return Header{}, nil, err
	}
	gatewayID, err := parquet_row.ReadString(r)
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
	return Header{ClientID: clientID, GatewayID: gatewayID}, row, nil
}

func appendString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}
