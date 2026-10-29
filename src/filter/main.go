package main

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-distribuidos/filter/filter"
)

func loadConfig() (filter.FilterConfig, error) {
	inputQueue := os.Getenv("INPUT_QUEUE")
	if inputQueue == "" {
		return filter.FilterConfig{}, errors.New("INPUT_QUEUE environment variable is required")
	}

	resultsExchange := os.Getenv("RESULTS_EXCHANGE")
	if resultsExchange == "" {
		return filter.FilterConfig{}, errors.New("RESULTS_EXCHANGE environment variable is required")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filter.FilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	return filter.FilterConfig{
		InputQueue:      inputQueue,
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

	worker, err := filter.NewFilter(config)
	if err != nil {
		slog.Error("While initializing filter", "err", err)
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
		slog.Error("Filter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
