package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-distribuidos/common/envconfig"
	"tp-distribuidos/sorter/sorter"
)

func loadConfig() (sorter.Config, error) {
	var env envconfig.Reader
	config := sorter.Config{
		InputReducerExchange:  env.String("INPUT_REDUCER_EXCHANGE"),
		SorterPrefix:          env.String("SORTER_PREFIX"),
		SorterID:              env.Int("SORTER_ID"),
		ReducerAmount:         env.Int("REDUCER_AMOUNT"),
		OutputGatewayExchange: env.String("OUTPUT_GATEWAY_EXCHANGE"),
		MomHost:               env.String("MOM_HOST"),
		MomPort:               env.Int("MOM_PORT"),
	}
	return config, env.Err()
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := sorter.NewSorter(config)
	if err != nil {
		slog.Error("While initializing sorter", "err", err)
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
		slog.Error("Sorter stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
