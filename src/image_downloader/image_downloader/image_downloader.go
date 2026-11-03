package image_downloader

import (
	"fmt"
	"log/slog"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
)

type Config struct {
	InputFilterExchange string
	IDownloaderPrefix   string
	IDownloaderID       int

	MomHost string
	MomPort int
}

type ImageDownloader struct {
	mom middleware.MultiRabbitMiddleware
}

func NewImageDownloader(config Config) (*ImageDownloader, error) {
	downloader := &ImageDownloader{}
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

// por ahora solo loguea la data
func (downloader *ImageDownloader) handleBiography(header inner.Header, biography inner.Biography) error {
	slog.Info("Biografia recibida",
		"client", header.ClientID,
		"gateway", header.GatewayID,
		"name", biography.Name,
		"url", biography.URL,
		"imageURL", biography.ImageURL)
	return nil
}

// todo: barrera por FilterAmount EOFs + EndOfResults(Q2) al gateway.
func (downloader *ImageDownloader) handleEndOfRecords(header inner.Header) error {
	slog.Info("End of records", "client", header.ClientID)
	return nil
}
