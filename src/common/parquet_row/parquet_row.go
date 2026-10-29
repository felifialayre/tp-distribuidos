package parquet_row

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	uint32Size   = 4
	uint64Size   = 8
	MaxFieldSize = 64 * 1024 * 1024 // 64 MB
)

var ErrFieldTooLarge = errors.New("parquet_row: field too large")

var ParquetColumns = []string{
	"abstract",
	"additional_entities",
	"date_created",
	"date_modified",
	"description",
	"event",
	"identifier",
	"image",
	"in_language",
	"infoboxes",
	"is_part_of",
	"license",
	"main_entity",
	"name",
	"references",
	"sections",
	"tables",
	"url",
	"version",
}

type ParquetRow struct {
	Abstract           string
	AdditionalEntities string
	DateCreated        string
	DateModified       string
	Description        string
	Event              string
	Identifier         int64
	Image              string
	InLanguage         string
	Infoboxes          string
	IsPartOf           string
	License            string
	MainEntity         string
	Name               string
	References         string
	Sections           string
	Tables             string
	URL                string
	Version            string
}

func (r *ParquetRow) StringFields() []*string {
	return []*string{
		&r.Abstract,
		&r.AdditionalEntities,
		&r.DateCreated,
		&r.DateModified,
		&r.Description,
		&r.Event,
		&r.Image,
		&r.InLanguage,
		&r.Infoboxes,
		&r.IsPartOf,
		&r.License,
		&r.MainEntity,
		&r.Name,
		&r.References,
		&r.Sections,
		&r.Tables,
		&r.URL,
		&r.Version,
	}
}

// uint64 size
// por cada col:
//
//		uint32  size row
//	    bytes   row
func (r *ParquetRow) Encode() ([]byte, error) {
	fields := r.StringFields()

	size := uint64Size
	for _, field := range fields {
		if len(*field) > MaxFieldSize {
			return nil, ErrFieldTooLarge
		}
		size += uint32Size + len(*field)
	}

	buf := make([]byte, 0, size)
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.Identifier))
	for _, field := range fields {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(*field)))
		buf = append(buf, *field...)
	}
	return buf, nil
}

func Decode(r io.Reader) (*ParquetRow, error) {
	var idBuf [uint64Size]byte
	if _, err := io.ReadFull(r, idBuf[:]); err != nil {
		return nil, err
	}

	row := &ParquetRow{Identifier: int64(binary.BigEndian.Uint64(idBuf[:]))}
	for _, field := range row.StringFields() {
		value, err := ReadString(r)
		if err != nil {
			return nil, err
		}
		*field = value
	}
	return row, nil
}

// esta funcion no se si deberia estar aca
func ReadString(r io.Reader) (string, error) {
	var sizeBuf [uint32Size]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return "", err
	}
	size := binary.BigEndian.Uint32(sizeBuf[:])
	if size > MaxFieldSize {
		return "", ErrFieldTooLarge
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
