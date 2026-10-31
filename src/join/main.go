package main

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-distribuidos/join/join"
)

func loadConfig() (join.Config, error) {
	inputStopwordsExchange := os.Getenv("INPUT_STOPWORDS_EXCHANGE")
	if inputStopwordsExchange == "" {
		return join.Config{}, errors.New("INPUT_STOPWORDS_EXCHANGE environment variable is required")
	}

	joinPrefix := os.Getenv("JOIN_PREFIX")
	if joinPrefix == "" {
		return join.Config{}, errors.New("JOIN_PREFIX environment variable is required")
	}

	joinID, err := strconv.Atoi(os.Getenv("JOIN_ID"))
	if err != nil {
		return join.Config{}, errors.New("JOIN_ID environment variable is required and must be a number")
	}

	stopwordsAmount, err := strconv.Atoi(os.Getenv("STOPWORDS_AMOUNT"))
	if err != nil {
		return join.Config{}, errors.New("STOPWORDS_AMOUNT environment variable is required and must be a number")
	}

	outputGatewayExchange := os.Getenv("OUTPUT_GATEWAY_EXCHANGE")
	if outputGatewayExchange == "" {
		return join.Config{}, errors.New("OUTPUT_GATEWAY_EXCHANGE environment variable is required")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return join.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return join.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	return join.Config{
		InputStopwordsExchange: inputStopwordsExchange,
		JoinPrefix:             joinPrefix,
		JoinID:                 joinID,
		StopwordsAmount:        stopwordsAmount,
		OutputGatewayExchange:  outputGatewayExchange,
		MomHost:                momHost,
		MomPort:                momPort,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := join.NewJoin(config)
	if err != nil {
		slog.Error("While initializing join", "err", err)
		return 1
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGTERM)
	go func() {
		<-sigterm
		slog.Info("SIGTERM recibido, cerrando")
		worker.Shutdown()
	}()

	if err := worker.Run(); err != nil {
		slog.Error("Join stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
