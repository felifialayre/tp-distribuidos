package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-distribuidos/common/envconfig"
	"tp-distribuidos/join_average/join_average"
)

func loadConfig() (join_average.Config, error) {
	var env envconfig.Reader
	config := join_average.Config{
		InputAccumulatorExchange:   env.String("INPUT_ACCUMULATOR_EXCHANGE"),
		JoinAveragePrefix:          env.String("JOIN_AVERAGE_PREFIX"),
		JoinAverageID:              env.Int("JOIN_AVERAGE_ID"),
		AccumulatorAmount:          env.Int("ACCUMULATOR_AMOUNT"),
		OutputAboveAverageExchange: env.String("OUTPUT_ABOVE_AVERAGE_EXCHANGE"),
		AboveAveragePrefix:         env.String("ABOVE_AVERAGE_PREFIX"),
		AboveAverageAmount:         env.Int("ABOVE_AVERAGE_AMOUNT"),
		MomHost:                    env.String("MOM_HOST"),
		MomPort:                    env.Int("MOM_PORT"),
	}
	return config, env.Err()
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := join_average.NewJoinAverage(config)
	if err != nil {
		slog.Error("While initializing join average", "err", err)
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
		slog.Error("Join average stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
