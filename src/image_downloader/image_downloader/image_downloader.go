package image_downloader

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
)

type Config struct {
	// info del nodo + input
	InputFilterExchange string
	IDownloaderPrefix   string
	IDownloaderID       int

	// cuantas replicas de filter mandan biografias: cada una manda su propio EOF
	FilterAmount int

	// resultado de Q2, directo al gateway
	OutputGatewayExchange string

	MomHost string
	MomPort int

	// data y metadata para el uso de wikimedia
	UserAgent          string
	HTTPTimeoutSeconds int
}

type ImageDownloader struct {
	mom                   middleware.MultiRabbitMiddleware
	fetcher               *Fetcher
	filterAmount          int
	outputGatewayExchange string
	eofsByClient          map[string]int
}

func NewImageDownloader(config Config) (*ImageDownloader, error) {
	if config.FilterAmount < 1 {
		return nil, errors.New("image downloader needs FILTER_AMOUNT")
	}
	if config.OutputGatewayExchange == "" {
		return nil, errors.New("image downloader needs OUTPUT_GATEWAY_EXCHANGE")
	}
	downloader := &ImageDownloader{
		fetcher:               NewFetcher(config.UserAgent, time.Duration(config.HTTPTimeoutSeconds)*time.Second),
		filterAmount:          config.FilterAmount,
		outputGatewayExchange: config.OutputGatewayExchange,
		eofsByClient:          make(map[string]int),
	}
	mom, err := downloader.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	downloader.mom = mom
	return downloader, nil
}

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
	if err := mom.AddOutputExchange(config.OutputGatewayExchange, middleware.Direct); err != nil {
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

// baja la imagen de la biografia y publica el resultado de Q2 al gateway. Si el
// articulo no tiene imagen (ImageURL == "") igual se publica con los bytes de imagen vacíos.
func (downloader *ImageDownloader) handleBiography(header inner.Header, biography inner.Biography) error {
	slog.Info("Biografia recibida",
		"client", header.ClientID,
		"gateway", header.GatewayID,
		"name", biography.Name,
		"url", biography.URL,
		"imageURL", biography.ImageURL)

	if biography.ImageURL == "" {
		slog.Info("Biografia sin imágen", "client", header.ClientID, "name", biography.Name)
		return downloader.publishResult(header, biography, nil)
	}

	imgBytes, err := downloader.fetcher.FetchImage(biography.ImageURL)
	if err != nil {
		// todo: si el error es por algo de la api se puede reintentar con backoff
		slog.Warn("No se pudo bajar la imagen", "client", header.ClientID, "url", biography.ImageURL, "err", err)
		return nil
	}

	slog.Info("Imagen descargada", "client", header.ClientID, "name", biography.Name, "bytes", len(imgBytes))
	return downloader.publishResult(header, biography, imgBytes)
}

func (downloader *ImageDownloader) publishResult(header inner.Header, biography inner.Biography, imgBytes []byte) error {
	result := inner.Q2Result{Name: biography.Name, URL: biography.URL, ImageBytes: imgBytes}
	body := inner.EncodeQ2Result(header, result)
	return downloader.mom.Publish(downloader.outputGatewayExchange, []string{header.GatewayID}, middleware.Message{Body: string(body)})
}

// handleEndOfRecords aplica la barrera: espera un EndOfRecords por cada replica de
// filter antes de dar por terminado el cliente. (El EndOfResults(Q2) al gateway va
// en la fase 3.)

// handleEndOfRecords usa una barrera por filterAmount para dar por terminado
func (downloader *ImageDownloader) handleEndOfRecords(header inner.Header) error {
	downloader.eofsByClient[header.ClientID]++
	if downloader.eofsByClient[header.ClientID] < downloader.filterAmount {
		return nil // faltan replicas de filter por terminar
	}
	delete(downloader.eofsByClient, header.ClientID)
	slog.Info("Q2 terminado", "client", header.ClientID)
	return nil
}
