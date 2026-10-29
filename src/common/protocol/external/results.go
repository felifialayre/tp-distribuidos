package external

import (
	"encoding/binary"

	"tp-distribuidos/common/parquet_row"
)

// --HABRIA QUE AGREGAR ACK Y BATCH--

// results: gateway -> cliente
// los resultados de cada consulta

// Formato:
//	uint32  opcode Result
//	uint32  query
//	uint32  cantidad de campos
//	        por cada campo: uint32 largo + bytes
//
// EndOfResults avisa que no hay mas resultados de una consulta
//	uint32  opcode EndOfResults
//	uint32  query

func (p *ExternalProtocol) WriteResult(query Query, fields []string) error {
	if len(fields) > maxResultFields {
		return ErrResultTooLarge
	}

	msg := binary.BigEndian.AppendUint32(nil, uint32(Result))
	msg = binary.BigEndian.AppendUint32(msg, uint32(query))
	msg = binary.BigEndian.AppendUint32(msg, uint32(len(fields)))
	for _, field := range fields {
		if len(field) > parquet_row.MaxFieldSize {
			return ErrFieldTooLarge
		}
		msg = binary.BigEndian.AppendUint32(msg, uint32(len(field)))
		msg = append(msg, field...)
	}
	return p.socket.Send(msg)
}

func (p *ExternalProtocol) ReadResult() (Query, []string, error) {
	query, err := p.readUint32()
	if err != nil {
		return 0, nil, err
	}
	count, err := p.readUint32()
	if err != nil {
		return 0, nil, err
	}
	if count > maxResultFields {
		return 0, nil, ErrResultTooLarge
	}

	fields := make([]string, 0, count)
	for range count {
		field, err := p.readString()
		if err != nil {
			return 0, nil, err
		}
		fields = append(fields, field)
	}
	return Query(query), fields, nil
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
