package client

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"tp-distribuidos/client/parquetreader"
	"tp-distribuidos/common/parquet_row"
	"tp-distribuidos/common/protocol/external"
	"tp-distribuidos/common/socket"
)

const (
	connectionAttempts        = 10
	connectionAttemptsDelayMs = 500
	connectionTimeout         = 5 * time.Second
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputFile  string
	BatchSize int
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
	go client.handleSignals()

	if err := client.sendParquet(); err != nil {
		if client.running.Load() {
			return err
		}
		return nil
	}

	return nil
}

func (client *Client) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	client.running.Store(false)
	client.proto.Close()
}

func (client *Client) sendParquet() error {
	reader, err := parquetreader.Open(client.config.InputFile)
	if err != nil {
		slog.Error("Error while opening input file", "err", err)
		return err
	}
	defer reader.Close()

	total := reader.NumRows()
	slog.Info("Sending parquet", "file", client.config.InputFile, "rows", total)

	batch := make([]*parquet_row.ParquetRow, 0, client.config.BatchSize)
	sent := 0
	for client.running.Load() {
		row, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			slog.Error("Error while reading parquet row", "err", err)
			return err
		}

		batch = append(batch, row)
		if len(batch) < client.config.BatchSize {
			continue
		}
		if err := client.sendBatch(batch); err != nil {
			return err
		}
		sent += len(batch)
		batch = batch[:0]
	}

	if len(batch) > 0 {
		if err := client.sendBatch(batch); err != nil {
			return err
		}
		sent += len(batch)
	}

	if err := client.proto.WriteEndOfRecords(); err != nil {
		return err
	}
	if err := client.proto.RecvACK(); err != nil {
		return err
	}

	slog.Info("Parquet sent", "rows", sent)
	return nil
}

func (client *Client) sendBatch(batch []*parquet_row.ParquetRow) error {
	if err := client.proto.WriteBatch(batch); err != nil {
		return err
	}
	return client.proto.RecvACK()
}
