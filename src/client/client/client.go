package client

import (
	"encoding/csv"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"tp-distribuidos/client/parquetreader"
	"tp-distribuidos/common/parquet_row"
	"tp-distribuidos/common/protocol/external"
	"tp-distribuidos/common/socket"
)

// directorio del OutputDir donde se guardan las imagenes de Q2
const q2ImagesDir = "q2_images"

const (
	connectionAttempts        = 10
	connectionAttemptsDelayMs = 500
	connectionTimeout         = 5 * time.Second
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputDir   string
	OutputDir  string
	BatchSize  int
}

type Client struct {
	proto   *external.ExternalProtocol
	running atomic.Bool
	config  ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	if config.BatchSize < 1 || config.BatchSize > external.MaxBatchRows {
		return nil, fmt.Errorf("batch size must be between 1 and %d, got %d", external.MaxBatchRows, config.BatchSize)
	}

	sock, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	client := &Client{
		proto:  external.NewExternalProtocol(sock),
		config: config,
	}
	client.running.Store(true)
	return client, nil
}

func connectToServer(host, port string) (*socket.Socket, error) {
	address := net.JoinHostPort(host, port)

	var sock *socket.Socket
	var err error
	for range connectionAttempts {
		sock, err = socket.Dial(address, connectionTimeout)
		if err == nil {
			return sock, nil
		}
		slog.Warn("Retrying connection...", "err", err)
		time.Sleep(connectionAttemptsDelayMs * time.Millisecond)
	}

	return nil, err
}

func (client *Client) Run() error {
	defer client.proto.Close()

	// el gateway manda los resultados a medida que los recibe, mientras el cliente todavia sube
	received := make(chan error, 1)
	go func() { received <- client.receiveResults() }()

	if err := client.sendParquet(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	if err := <-received; err != nil && client.running.Load() {
		return err
	} //espero a que el receiver
	return nil
}

// consultas que se esperan del gateway; cada una se escribe en su propio csv
var queries = []external.Query{external.Q1, external.Q2, external.Q3, external.Q4, external.Q5}

// le llegan los results del gateway
func (client *Client) receiveResults() error {
	if err := os.MkdirAll(client.config.OutputDir, 0o755); err != nil {
		return err
	}
	// las imagenes de Q2 van a un directorio aparte, en el csv solo queda la ruta
	imagesDir := filepath.Join(client.config.OutputDir, q2ImagesDir)
	if err := os.MkdirAll(imagesDir, 0o755); err != nil {
		return err
	}
	outputs := make(map[external.Query]*csv.Writer, len(queries))
	for _, query := range queries {
		file, err := os.Create(filepath.Join(client.config.OutputDir, fmt.Sprintf("q%d.csv", query)))
		if err != nil {
			return err
		}
		defer file.Close()
		outputs[query] = csv.NewWriter(file)
	}
	// q2 lleva header porque sus columnas tienen significado y se navega a mano
	if err := outputs[external.Q2].Write([]string{"name", "url", "path"}); err != nil {
		return err
	}

	rows := 0
	for len(outputs) > 0 {
		msgType, err := client.proto.ReadMsgType()
		if err != nil {
			return err
		}

		switch msgType {
		case external.Result:
			query, fields, err := client.proto.ReadResult()
			if err != nil {
				return err
			}
			output, ok := outputs[query]
			if !ok {
				return fmt.Errorf("unexpected query %d", query)
			}
			if query == external.Q2 {
				err = client.writeQ2Result(fields, imagesDir, output)
			} else {
				err = output.Write(fields)
			}
			if err != nil {
				return err
			}
			rows++
		case external.EndOfResults:
			query, err := client.proto.ReadEndOfResults()
			if err != nil {
				return err
			}
			output, ok := outputs[query]
			if !ok {
				return fmt.Errorf("unexpected end of query %d", query)
			}
			output.Flush()
			if err := output.Error(); err != nil {
				return err
			}
			delete(outputs, query) // esta query ya termino
			slog.Info("Query finished", "query", query)
		default:
			return fmt.Errorf("unexpected message type %d", msgType)
		}
	}

	slog.Info("Results received", "rows", rows)
	return nil
}

// writeQ2Result guarda la imagen (si la hay) en imagesDir y escribe una fila
// name,url,path en el csv. Si el articulo no tenia imagen, path queda vacio.
func (client *Client) writeQ2Result(fields []string, imagesDir string, out *csv.Writer) error {
	if len(fields) != 3 {
		return fmt.Errorf("q2 result expects 3 fields, got %d", len(fields))
	}
	name, url, imageBytes := fields[0], fields[1], []byte(fields[2])

	path := ""
	if len(imageBytes) > 0 {
		fileName := fmt.Sprintf("%08x%s", crc32.ChecksumIEEE([]byte(url)), imageExtension(imageBytes))
		if err := os.WriteFile(filepath.Join(imagesDir, fileName), imageBytes, 0o644); err != nil {
			return err
		}
		path = filepath.Join(q2ImagesDir, fileName)
	}
	return out.Write([]string{name, url, path})
}

// imageExtension adivina la extension a partir del contenido, no es necesario
// pero usa un algoritmo q lee los primeros bytes y predice. Sirve para conservar
// gifs por ejemplo si es q hay.
func imageExtension(data []byte) string {
	switch http.DetectContentType(data) {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	default:
		return ".img"
	}
}

func (client *Client) Shutdown() {
	client.running.Store(false)
	client.proto.Close()
}

// manda las filas de todos los parquet del directorio, en batches, y al final el EndOfRecords
func (client *Client) sendParquet() error {
	files, err := parquetFiles(client.config.InputDir)
	if err != nil {
		slog.Error("Error while listing input files", "err", err)
		return err
	}

	batch := make([]*parquet_row.ParquetRow, 0, client.config.BatchSize)
	sent := 0
	for _, file := range files {
		if !client.running.Load() {
			break
		}
		reader, err := parquetreader.Open(file)
		if err != nil {
			slog.Error("Error while opening input file", "file", file, "err", err)
			return err
		}
		slog.Info("Sending parquet", "file", file, "rows", reader.NumRows())

		for client.running.Load() {
			row, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				slog.Error("Error while reading parquet row", "err", err)
				reader.Close()
				return err
			}

			batch = append(batch, row)
			if len(batch) < client.config.BatchSize {
				continue
			}
			if err := client.proto.WriteBatch(batch); err != nil {
				reader.Close()
				return err
			}
			sent += len(batch)
			batch = batch[:0]
		}
		reader.Close() // se cierra antes de pasar al proximo archivo
	}

	if !client.running.Load() {
		return nil
	}

	if len(batch) > 0 {
		if err := client.proto.WriteBatch(batch); err != nil {
			return err
		}
		sent += len(batch)
	}

	if err := client.proto.WriteEndOfRecords(); err != nil {
		return err
	}

	slog.Info("Parquet sent", "rows", sent)
	return nil
}

// parquetFiles devuelve los .parquet del directorio
func parquetFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".parquet" {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no parquet files in %s", dir)
	}
	return files, nil
}
