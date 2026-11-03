package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-distribuidos/accumulator/accumulator"
	"tp-distribuidos/common/envconfig"
)

func loadConfig() (accumulator.Config, error) {
	var env envconfig.Reader
	config := accumulator.Config{
		InputStopwordsExchange: env.String("INPUT_STOPWORDS_EXCHANGE"),
		AccumulatorPrefix:      env.String("ACCUMULATOR_PREFIX"),
		AccumulatorID:          env.Int("ACCUMULATOR_ID"),
		StopwordsAmount:        env.Int("STOPWORDS_AMOUNT"),
		OutputReducerExchange:  env.String("OUTPUT_REDUCER_EXCHANGE"),
		ReducerPrefix:          env.String("REDUCER_PREFIX"),
		ReducerAmount:          env.Int("REDUCER_AMOUNT"),
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

	worker, err := accumulator.NewAccumulator(config)
	if err != nil {
		slog.Error("While initializing accumulator", "err", err)
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
		slog.Error("Accumulator stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
