package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-distribuidos/common/envconfig"
	"tp-distribuidos/reducer/reducer"
)

func loadConfig() (reducer.Config, error) {
	var env envconfig.Reader
	config := reducer.Config{
		InputAccumulatorExchange: env.String("INPUT_ACCUMULATOR_EXCHANGE"),
		ReducerPrefix:            env.String("REDUCER_PREFIX"),
		ReducerID:                env.Int("REDUCER_ID"),
		AccumulatorAmount:        env.Int("ACCUMULATOR_AMOUNT"),
		OutputSorterExchange:     env.String("OUTPUT_SORTER_EXCHANGE"),
		SorterPrefix:             env.String("SORTER_PREFIX"),
		SorterAmount:             env.Int("SORTER_AMOUNT"),
		MomHost:                  env.String("MOM_HOST"),
		MomPort:                  env.Int("MOM_PORT"),
	}
	return config, env.Err()
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := reducer.NewReducer(config)
	if err != nil {
		slog.Error("While initializing reducer", "err", err)
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
		slog.Error("Reducer stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
