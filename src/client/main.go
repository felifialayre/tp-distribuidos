package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-distribuidos/client/client"
)

const _DEFAULT_BATCH_SIZE = 100

func loadConfig() (client.ClientConfig, error) {
	host := os.Getenv("SERVER_HOST")
	if host == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	port := os.Getenv("SERVER_PORT")
	if port == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE environment variable is required")
	}

	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE environment variable is required")
	}

	batchSize := _DEFAULT_BATCH_SIZE
	if value := os.Getenv("BATCH_SIZE"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil {
			return client.ClientConfig{}, fmt.Errorf("BATCH_SIZE must be a number: %w", err)
		}
		batchSize = n
	}

	return client.ClientConfig{
		ServerHost: host,
		ServerPort: port,
		InputFile:  inputFile,
		OutputFile: outputFile,
		BatchSize:  batchSize,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	c, err := client.NewClient(config)
	if err != nil {
		slog.Error("While connecting to server", "err", err)
		return 1
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGTERM)
	go func() {
		<-sigterm
		slog.Info("SIGTERM recibido, cerrando")
		c.Shutdown()
	}()

	if err := c.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
