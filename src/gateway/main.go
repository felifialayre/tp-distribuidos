package main

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"tp-distribuidos/gateway/gateway"
)

func loadConfig() (gateway.GatewayConfig, error) {
	gatewayID := os.Getenv("GATEWAY_ID")
	if gatewayID == "" {
		return gateway.GatewayConfig{}, errors.New("GATEWAY_ID environment variable is required")
	}

	serverPort, err := strconv.Atoi(os.Getenv("SERVER_PORT"))
	if err != nil {
		return gateway.GatewayConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	filterExchange := os.Getenv("OUTPUT_FILTER_EXCHANGE")
	if filterExchange == "" {
		return gateway.GatewayConfig{}, errors.New("OUTPUT_FILTER_EXCHANGE environment variable is required")
	}

	filterPrefix := os.Getenv("FILTER_PREFIX")
	if filterPrefix == "" {
		return gateway.GatewayConfig{}, errors.New("FILTER_PREFIX environment variable is required")
	}

	filterAmount, err := strconv.Atoi(os.Getenv("FILTER_AMOUNT"))
	if err != nil || filterAmount < 1 {
		return gateway.GatewayConfig{}, errors.New("FILTER_AMOUNT environment variable is required and must be a positive number")
	}

	joinAmount, err := strconv.Atoi(os.Getenv("JOIN_AMOUNT"))
	if err != nil || joinAmount < 1 {
		return gateway.GatewayConfig{}, errors.New("JOIN_AMOUNT environment variable is required and must be a positive number")
	}

	resultsExchange := os.Getenv("INPUT_RESULTS_EXCHANGE")
	if resultsExchange == "" {
		return gateway.GatewayConfig{}, errors.New("INPUT_RESULTS_EXCHANGE environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return gateway.GatewayConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return gateway.GatewayConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	return gateway.GatewayConfig{
		GatewayID:       gatewayID,
		ServerPort:      serverPort,
		FilterExchange:  filterExchange,
		FilterPrefix:    filterPrefix,
		FilterAmount:    filterAmount,
		JoinAmount:      joinAmount,
		ResultsExchange: resultsExchange,
		MomHost:         momHost,
		MomPort:         momPort,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	server, err := gateway.NewGateway(config)
	if err != nil {
		slog.Error("While initializing gateway", "err", err)
		return 1
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGTERM)
	go func() {
		<-sigterm
		slog.Info("SIGTERM recibido, cerrando")
		server.Shutdown()
	}()

	if err := server.Run(); err != nil {
		slog.Error("Gateway stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
