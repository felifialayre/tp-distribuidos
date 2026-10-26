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

	outputQueueName := os.Getenv("OUTPUT_QUEUE")
	if outputQueueName == "" {
		return gateway.GatewayConfig{}, errors.New("OUTPUT_QUEUE environment variable is required")
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
		OutputQueueName: outputQueueName,
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
