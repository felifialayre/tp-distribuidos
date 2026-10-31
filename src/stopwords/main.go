package main

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"tp-distribuidos/stopwords/stopwords"
)

func loadConfig() (stopwords.Config, error) {
	inputFilterExchange := os.Getenv("INPUT_FILTER_EXCHANGE")
	if inputFilterExchange == "" {
		return stopwords.Config{}, errors.New("INPUT_FILTER_EXCHANGE environment variable is required")
	}

	stopwordsPrefix := os.Getenv("STOPWORDS_PREFIX")
	if stopwordsPrefix == "" {
		return stopwords.Config{}, errors.New("STOPWORDS_PREFIX environment variable is required")
	}

	stopwordsID, err := strconv.Atoi(os.Getenv("STOPWORDS_ID"))
	if err != nil {
		return stopwords.Config{}, errors.New("STOPWORDS_ID environment variable is required and must be a number")
	}

	filterAmount, err := strconv.Atoi(os.Getenv("FILTER_AMOUNT"))
	if err != nil {
		return stopwords.Config{}, errors.New("FILTER_AMOUNT environment variable is required and must be a number")
	}

	joinAmount, _ := strconv.Atoi(os.Getenv("JOIN_AMOUNT"))

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return stopwords.Config{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return stopwords.Config{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	return stopwords.Config{
		InputFilterExchange: inputFilterExchange,
		StopwordsPrefix:     stopwordsPrefix,
		StopwordsID:         stopwordsID,
		FilterAmount:        filterAmount,
		OutputJoinExchange:  os.Getenv("OUTPUT_JOIN_EXCHANGE"),
		JoinPrefix:          os.Getenv("JOIN_PREFIX"),
		JoinAmount:          joinAmount,
		MomHost:             momHost,
		MomPort:             momPort,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := stopwords.NewStopwordsFilter(config)
	if err != nil {
		slog.Error("While initializing stopwords", "err", err)
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
		slog.Error("Stopwords stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
