package client

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"tp-distribuidos/client/parquetreader"
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
}

type Client struct {
	sock    *socket.Socket
	proto   *external.ExternalProtocol
	running atomic.Bool
	config  ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	sock, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	client := &Client{
		sock:   sock,
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
	defer client.sock.Close()
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
	client.sock.Close()
}

func (client *Client) expectMsgType(expectedMsgType external.MsgType) error {
	msgType, err := client.proto.ReadMsgType()
	if err != nil {
		slog.Debug("Error while reading message type", "err", err)
		return err
	}
	if msgType != expectedMsgType {
		return errors.New("unexpected message type")
	}
	return nil
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

		if err := client.proto.WriteRecord(row); err != nil {
			return err
		}
		if err := client.expectMsgType(external.Ack); err != nil {
			return err
		}
		sent++
	}

	if err := client.proto.WriteEndOfRecords(); err != nil {
		return err
	}
	if err := client.expectMsgType(external.Ack); err != nil {
		return err
	}

	slog.Info("Parquet sent", "rows", sent)
	return nil
}
