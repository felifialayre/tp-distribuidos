package main

import (
	"errors"
	"log/slog"
	"os"

	"tp-distribuidos/client/client"
)

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

	return client.ClientConfig{
		ServerHost: host,
		ServerPort: port,
		InputFile:  inputFile,
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

	if err := c.Run(); err != nil {
		slog.Error("Client stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
