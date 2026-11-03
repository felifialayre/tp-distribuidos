package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"tp-distribuidos/common/envconfig"
	"tp-distribuidos/image_downloader/image_downloader"
)

func loadConfig() (image_downloader.Config, error) {
	var env envconfig.Reader
	config := image_downloader.Config{
		InputFilterExchange: env.String("INPUT_FILTER_EXCHANGE"),
		IDownloaderPrefix:   env.String("IDOWNLOADER_PREFIX"),
		IDownloaderID:       env.Int("IDOWNLOADER_ID"),
		MomHost:             env.String("MOM_HOST"),
		MomPort:             env.Int("MOM_PORT"),
		UserAgent:           env.String("USER_AGENT"),
		HTTPTimeoutSeconds:  env.Int("HTTP_TIMEOUT_SECONDS"),
		DownloadDir:         env.String("DOWNLOAD_DIR"),
	}
	return config, env.Err()
}

// por ahora copio estructura del filter
func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("While loading config", "err", err)
		return 1
	}

	worker, err := image_downloader.NewImageDownloader(config)
	if err != nil {
		slog.Error("While initializing image downloader", "err", err)
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
		slog.Error("Image downloader stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
