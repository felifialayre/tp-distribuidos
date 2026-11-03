package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-distribuidos/above_average/above_average"
	"tp-distribuidos/common/envconfig"
)

func loadConfig() (above_average.Config, error) {
	var env envconfig.Reader
	config := above_average.Config{
		InputStopwordsExchange: env.String("INPUT_STOPWORDS_EXCHANGE"),
		InputAverageExchange:   env.String("INPUT_AVERAGE_EXCHANGE"),
		AboveAveragePrefix:     env.String("ABOVE_AVERAGE_PREFIX"),
		AboveAverageID:         env.Int("ABOVE_AVERAGE_ID"),
		StopwordsAmount:        env.Int("STOPWORDS_AMOUNT"),
		OutputGatewayExchange:  env.String("OUTPUT_GATEWAY_EXCHANGE"),
		MomHost:                env.String("MOM_HOST"),
		MomPort:                env.Int("MOM_PORT"),
	}
	return config, env.Err()
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := above_average.NewAboveAverageFilter(config)
	if err != nil {
		slog.Error("While initializing above average filter", "err", err)
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
		slog.Error("Above average filter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
