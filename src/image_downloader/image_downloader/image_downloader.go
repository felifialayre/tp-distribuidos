package image_downloader

import (
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"time"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
)

type Config struct {
	// info del nodo + input
	InputFilterExchange string
	IDownloaderPrefix   string
	IDownloaderID       int

	MomHost string
	MomPort int

	// data y metadata para el uso de wikimedia
	UserAgent          string
	HTTPTimeoutSeconds int
	DownloadDir        string
}

type ImageDownloader struct {
	mom         middleware.MultiRabbitMiddleware
	fetcher     *Fetcher
	downloadDir string
}

func NewImageDownloader(config Config) (*ImageDownloader, error) {
	downloader := &ImageDownloader{
		fetcher:     NewFetcher(config.UserAgent, time.Duration(config.HTTPTimeoutSeconds)*time.Second),
		downloadDir: config.DownloadDir,
	}
	if config.DownloadDir != "" {
		if err := os.MkdirAll(config.DownloadDir, 0o755); err != nil {
			return nil, err
		}
	}
	mom, err := downloader.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	downloader.mom = mom
	return downloader, nil
}

// en principio solo se conecta al input
func (downloader *ImageDownloader) setUpRabbitCommunication(config Config) (middleware.MultiRabbitMiddleware, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s_%d", config.IDownloaderPrefix, config.IDownloaderID)
	if err := mom.AddInputExchange(config.InputFilterExchange, middleware.Direct, []string{key}, downloader.callback); err != nil {
		mom.Close()
		return nil, err
	}
	return mom, nil
}

func (downloader *ImageDownloader) Run() error {
	defer downloader.mom.Close()
	return downloader.mom.StartConsuming()
}

func (downloader *ImageDownloader) Shutdown() {
	_ = downloader.mom.StopConsuming()
}

func (downloader *ImageDownloader) callback(msg middleware.Message, ack func(), nack func()) {
	if err := downloader.handleMessage([]byte(msg.Body)); err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (downloader *ImageDownloader) handleMessage(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}

	switch op {
	case inner.OpBiography:
		header, biography, err := inner.DecodeBiography(data)
		if err != nil {
			return err
		}
		return downloader.handleBiography(header, biography)
	case inner.OpEndOfRecords:
		header, err := inner.DecodeEndOfRecords(data)
		if err != nil {
			return err
		}
		return downloader.handleEndOfRecords(header)
	default:
		return inner.ErrUnexpectedOpcode
	}
}

// baja la imagen de la biografia y la guarda a disco
func (downloader *ImageDownloader) handleBiography(header inner.Header, biography inner.Biography) error {
	slog.Info("Biografia recibida",
		"client", header.ClientID,
		"gateway", header.GatewayID,
		"name", biography.Name,
		"url", biography.URL,
		"imageURL", biography.ImageURL)

	if biography.ImageURL == "" {
		slog.Info("Biografia sin imágen")
		return nil
	}

	imgBytes, err := downloader.fetcher.FetchImage(biography.ImageURL)
	if err != nil {
		// todo: si el error es por algo de la api se puede reintentar con backoff
		slog.Warn("No se pudo bajar la imagen", "client", header.ClientID, "url", biography.ImageURL, "err", err)
		return nil
	}

	if err := downloader.saveImage(biography, imgBytes); err != nil {
		slog.Error("No se pudo guardar la imagen", "url", biography.URL, "err", err)
		return nil
	}

	slog.Info("Imagen guardada", "client", header.ClientID, "name", biography.Name, "bytes", len(imgBytes))
	return nil
}

// saveImage guarda los bytes en downloadDir. El nombre se deriva del hash de la
// URL de la imágen para evitar cosas raras con tildes, colisiones, etc.
func (downloader *ImageDownloader) saveImage(biography inner.Biography, imgBytes []byte) error {
	ext := path.Ext(biography.ImageURL)
	name := fmt.Sprintf("%08x%s", crc32.ChecksumIEEE([]byte(biography.URL)), ext)
	return os.WriteFile(filepath.Join(downloader.downloadDir, name), imgBytes, 0o644)
}

// todo: barrera por FilterAmount EOFs + EndOfResults(Q2) al gateway.
func (downloader *ImageDownloader) handleEndOfRecords(header inner.Header) error {
	slog.Info("End of records", "client", header.ClientID)
	return nil
}
