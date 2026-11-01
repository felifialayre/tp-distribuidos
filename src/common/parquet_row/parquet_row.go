package parquet_row

import (
	"encoding/binary"
	"errors"
	"io"
	"time"
)

const (
	uint32Size   = 4
	uint64Size   = 8
	MaxFieldSize = 64 * 1024 * 1024 // 64 MB
	MaxRowSize   = 64 * 1024 * 1024 // 64 MB
)

var (
	ErrFieldTooLarge = errors.New("parquet_row: field too large")
	ErrRowTooLarge   = errors.New("parquet_row: row too large")
)

// DateLayout es el formato de DateCreated y DateModified. En el parquet son
// TIMESTAMP en milisegundos sin zona horaria, por eso el formato no la lleva.
const DateLayout = "2006-01-02T15:04:05"

func ParseDate(s string) (time.Time, error) {
	return time.Parse(DateLayout, s)
}

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

type Language struct {
	Identifier string
}

type Image struct {
	ContentURL string
	Height     int64
	Width      int64
}

type Section struct {
	Type     string
	Name     string
	HasParts string
}

func (s *Section) Fields() []*string {
	return []*string{&s.Type, &s.Name, &s.HasParts}
}

type ParquetRow struct {
	Abstract           string
	AdditionalEntities string
	DateCreated        string
	DateModified       string
	Description        string
	Event              string
	Identifier         int64
	Image              Image
	InLanguage         Language
	Infoboxes          string
	IsPartOf           string
	License            string
	MainEntity         string
	Name               string
	References         string
	Sections           []Section
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
		&r.Image.ContentURL,
		&r.InLanguage.Identifier,
		&r.Infoboxes,
		&r.IsPartOf,
		&r.License,
		&r.MainEntity,
		&r.Name,
		&r.References,
		&r.Tables,
		&r.URL,
		&r.Version,
	}
}

func (r *ParquetRow) SectionNames() []string {
	names := make([]string, 0, len(r.Sections))
	for _, section := range r.Sections {
		names = append(names, section.Name)
	}
	return names
}

// uint64 identifier
// por cada string de StringFields:
//
//	uint32  size
//	bytes
//
// uint64 image height
// uint64 image width
// uint32 cantidad de secciones, y por cada uno sus fields (uint32 size + bytes)
func (r *ParquetRow) Encode() ([]byte, error) {
	fields := r.StringFields()
	for i := range r.Sections {
		fields = append(fields, r.Sections[i].Fields()...)
	}

	size := 3*uint64Size + uint32Size
	for _, field := range fields {
		if len(*field) > MaxFieldSize {
			return nil, ErrFieldTooLarge
		}
		size += uint32Size + len(*field)
	}
	if size > MaxRowSize {
		return nil, ErrRowTooLarge
	}

	buf := make([]byte, 0, size)
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.Identifier))
	for _, field := range r.StringFields() {
		buf = appendField(buf, *field)
	}
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.Image.Height))
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.Image.Width))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Sections)))
	for i := range r.Sections {
		for _, field := range r.Sections[i].Fields() {
			buf = appendField(buf, *field)
		}
	}
	return buf, nil
}

func appendField(buf []byte, field string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(field)))
	return append(buf, field...)
}

func Decode(r io.Reader) (*ParquetRow, error) {
	var buf [uint64Size]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, err
	}
	row := &ParquetRow{Identifier: int64(binary.BigEndian.Uint64(buf[:]))}

	//primero carga los campos que son strings
	for _, field := range row.StringFields() {
		value, err := readString(r)
		if err != nil {
			return nil, err
		}
		*field = value
	}

	//despues cargas los campos numericos
	for _, number := range []*int64{&row.Image.Height, &row.Image.Width} {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return nil, err
		}
		*number = int64(binary.BigEndian.Uint64(buf[:]))
	}

	//al final carga las sections
	if _, err := io.ReadFull(r, buf[:uint32Size]); err != nil {
		return nil, err
	}
	count := binary.BigEndian.Uint32(buf[:uint32Size])
	for range count {
		var section Section
		for _, field := range section.Fields() {
			value, err := readString(r)
			if err != nil {
				return nil, err
			}
			*field = value
		}
		row.Sections = append(row.Sections, section)
	}
	return row, nil
}

func readString(r io.Reader) (string, error) {
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
