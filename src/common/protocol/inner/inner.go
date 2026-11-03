package inner

import (
	"bytes"
	"errors"
	"io"
)

// tipo de mensaje
type Opcode uint8

const (
	OpRecord Opcode = iota + 1
	OpEndOfRecords
	OpQ1Result
	OpEndOfResults
	OpAbstract
	OpBiography
	OpLangWords
	OpQ3Result
	OpWords
	OpWordCounts
	OpQ4Result
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

	clientID, err := readString(r)
	if err != nil {
		return Header{}, err
	}
	gatewayID, err := readString(r)
	if err != nil {
		return Header{}, err
	}
	return Header{ClientID: clientID, GatewayID: gatewayID}, nil
}

// Lo usan todos los nodos para propagar el fin hacia adelante

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
