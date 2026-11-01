package parquetreader

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"

	"tp-distribuidos/common/parquet_row"
)

type Reader struct {
	file    *os.File
	reader  *parquet.Reader
	columns []string
}

func Open(path string) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parquetreader: open %s: %w", path, err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("parquetreader: stat %s: %w", path, err)
	}

	//parquet.OpenFile maneja todos los problemas de parquet (con la metadata al final)
	// lo deja en un formato de Go mucho mas manejable
	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("parquetreader: parse %s: %w", path, err)
	}

	fields := pf.Schema().Fields()
	columns := make([]string, len(fields))
	have := make(map[string]bool, len(fields))
	for i, field := range fields {
		columns[i] = field.Name()
		have[field.Name()] = true
	}

	for _, name := range parquet_row.ParquetColumns {
		if !have[name] {
			file.Close()
			return nil, fmt.Errorf("parquetreader: %s: missing column %q", path, name)
		}
	}

	return &Reader{
		file:    file,
		reader:  parquet.NewReader(pf),
		columns: columns,
	}, nil
}

func (r *Reader) Columns() []string {
	return r.columns
}

func (r *Reader) NumRows() int64 {
	return r.reader.NumRows()
}

func (r *Reader) Next() (*parquet_row.ParquetRow, error) {
	record := make(map[string]any, len(r.columns))
	if err := r.reader.Read(&record); err != nil {
		return nil, err
	}

	var firstErr error

	//devuelve la columna como string
	text := func(name string) string {
		value, err := toString(record[name])
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("parquetreader: column %q: %w", name, err)
		}
		return value
	}

	// para columnas que son grupo, devuelve un map con los campos de adentro
	group := func(name string) map[string]any {
		fields, _ := record[name].(map[string]any) // nil si la columna es nula
		return fields
	}

	//devuelve un campo dentro de un grupo
	groupText := func(name, field string) string {
		value, err := toString(group(name)[field])
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("parquetreader: column %q.%q: %w", name, field, err)
		}
		return value
	}

	//lo mismo que groupText pero para enteros (dentro de grupos)
	groupInt := func(name, field string) int64 {
		switch v := group(name)[field].(type) {
		case nil:
			return 0
		case int64:
			return v
		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("parquetreader: column %q.%q: unexpected type %T", name, field, v)
			}
			return 0
		}
	}

	// date convierte un TIMESTAMP en milisegundos al formato de parquet_row
	date := func(name string) string {
		switch v := record[name].(type) {
		case nil:
			return ""
		case int64:
			return time.UnixMilli(v).UTC().Format(parquet_row.DateLayout)
		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("parquetreader: column %q: unexpected type %T", name, v)
			}
			return ""
		}
	}

	// sections ya viene como texto JSON en el parquet
	sections, err := parseSections(text("sections"))
	if err != nil && firstErr == nil {
		firstErr = fmt.Errorf("parquetreader: column \"sections\": %w", err)
	}

	identifier, ok := record["identifier"].(int64)
	if !ok {
		return nil, fmt.Errorf("parquetreader: column \"identifier\": unexpected type %T", record["identifier"])
	}

	row := &parquet_row.ParquetRow{
		Abstract:           text("abstract"),
		AdditionalEntities: text("additional_entities"),
		DateCreated:        date("date_created"),
		DateModified:       date("date_modified"),
		Description:        text("description"),
		Event:              text("event"),
		Identifier:         identifier,
		Image: parquet_row.Image{
			ContentURL: groupText("image", "content_url"),
			Height:     groupInt("image", "height"),
			Width:      groupInt("image", "width"),
		},
		InLanguage: parquet_row.Language{Identifier: groupText("in_language", "identifier")},
		Infoboxes:  text("infoboxes"),
		IsPartOf:   text("is_part_of"),
		License:    text("license"),
		MainEntity: text("main_entity"),
		Name:       text("name"),
		References: text("references"),
		Sections:   sections,
		Tables:     text("tables"),
		URL:        text("url"),
		Version:    text("version"),
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return row, nil
}

func (r *Reader) Close() error {
	_ = r.reader.Close()
	return r.file.Close()
}

func toString(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano), nil
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
}

// parseSections separa las secciones de primer nivel, lo anidado (has_parts) queda en JSON
func parseSections(text string) ([]parquet_row.Section, error) {
	if text == "" {
		return nil, nil
	}
	var parsed []struct {
		Type     string          `json:"type"`
		Name     string          `json:"name"`
		HasParts json.RawMessage `json:"has_parts"`
	}
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		return nil, err
	}
	sections := make([]parquet_row.Section, 0, len(parsed))
	for _, section := range parsed {
		sections = append(sections, parquet_row.Section{
			Type:     section.Type,
			Name:     section.Name,
			HasParts: string(section.HasParts),
		})
	}
	return sections, nil
}
