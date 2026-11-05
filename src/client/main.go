package main

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"tp-distribuidos/client/client"
)

const _DEFAULT_BATCH_SIZE = 100

func loadConfig() (client.ClientConfig, error) {
	gateways, err := parseGatewayAddresses(os.Getenv("GATEWAY_ADDRESSES"))
	if err != nil {
		return client.ClientConfig{}, err
	}

	inputDir := os.Getenv("INPUT_DIR")
	if inputDir == "" {
		return client.ClientConfig{}, errors.New("INPUT_DIR environment variable is required")
	}

	outputDir := os.Getenv("OUTPUT_DIR")
	if outputDir == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_DIR environment variable is required")
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
		GatewayAddresses: gateways,
		InputDir:         inputDir,
		OutputDir:        outputDir,
		BatchSize:        batchSize,
	}, nil
}

func parseGatewayAddresses(value string) ([]string, error) {
	var addresses []string
	for _, address := range strings.Split(value, ",") {
		address = strings.TrimSpace(address)
		if address == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(address); err != nil {
			return nil, fmt.Errorf("GATEWAY_ADDRESSES has an invalid address %q (expected host:port): %w", address, err)
		}
		addresses = append(addresses, address)
	}
	if len(addresses) == 0 {
		return nil, errors.New("GATEWAY_ADDRESSES environment variable is required (comma separated host:port list)")
	}
	return addresses, nil
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
