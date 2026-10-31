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
	text := func(name string) string {
		value, err := toString(record[name])
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("parquetreader: column %q: %w", name, err)
		}
		return value
	}

	// martino: igual medio que estamos sacando cosas aca....no deberiamos xd pero bueno
	// child lee un campo de una columna que es un grupo (in_language, image)
	child := func(name, field string) string {
		group, ok := record[name].(map[string]any)
		if !ok {
			return ""
		}
		value, err := toString(group[field])
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("parquetreader: column %q.%q: %w", name, field, err)
		}
		return value
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
		Image:              child("image", "content_url"),
		InLanguage:         child("in_language", "identifier"),
		Infoboxes:          text("infoboxes"),
		IsPartOf:           text("is_part_of"),
		License:            text("license"),
		MainEntity:         text("main_entity"),
		Name:               text("name"),
		References:         text("references"),
		Sections:           text("sections"),
		Tables:             text("tables"),
		URL:                text("url"),
		Version:            text("version"),
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
