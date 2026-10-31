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
	inputGatewayExchange := os.Getenv("INPUT_GATEWAY_EXCHANGE")
	if inputGatewayExchange == "" {
		return filter.FilterConfig{}, errors.New("INPUT_GATEWAY_EXCHANGE environment variable is required")
	}

	filterPrefix := os.Getenv("FILTER_PREFIX")
	if filterPrefix == "" {
		return filter.FilterConfig{}, errors.New("FILTER_PREFIX environment variable is required")
	}

	filterID, err := strconv.Atoi(os.Getenv("FILTER_ID"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("FILTER_ID environment variable is required and must be a number")
	}

	outputGatewayExchange := os.Getenv("OUTPUT_GATEWAY_EXCHANGE")
	if outputGatewayExchange == "" {
		return filter.FilterConfig{}, errors.New("OUTPUT_GATEWAY_EXCHANGE environment variable is required")
	}

	// solo los usa el tipo de filter que corresponda; NewFilter valida que esten
	stopwordsAmount, _ := strconv.Atoi(os.Getenv("STOPWORDS_AMOUNT"))
	q2Amount, _ := strconv.Atoi(os.Getenv("IDOWNLOADER_AMOUNT"))

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return filter.FilterConfig{}, errors.New("MOM_HOST environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return filter.FilterConfig{}, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	return filter.FilterConfig{
		InputGatewayExchange:      inputGatewayExchange,
		FilterPrefix:              filterPrefix,
		FilterID:                  filterID,
		OutputStopwordsExchange:   os.Getenv("OUTPUT_STOPWORDS_EXCHANGE"),
		StopwordsPrefix:           os.Getenv("STOPWORDS_PREFIX"),
		StopwordsAmount:           stopwordsAmount,
		OutputIDownloaderExchange: os.Getenv("OUTPUT_IDOWNLOADER_EXCHANGE"),
		Q2Prefix:                  os.Getenv("IDOWNLOADER_PREFIX"),
		Q2Amount:                  q2Amount,
		OutputGatewayExchange:     outputGatewayExchange,
		MomHost:                   momHost,
		MomPort:                   momPort,
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
