package image_downloader

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
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

	// cantidad de goroutines que bajan imágenes en paralelo
	Workers int

	// resultado de Q2, directo al gateway
	OutputGatewayExchange string

	MomHost string
	MomPort int

	// data y metadata para el uso de wikimedia
	UserAgent          string
	HTTPTimeoutSeconds int
}

// job es una tarea (descarga de imágen) pendiente,
// viene con el ack para que cada subrutina lo llame al terminar
type job struct {
	header    inner.Header
	biography inner.Biography
	ack       func()
}

// clientState sirve para la barrera por cliente, con concurrencia en descargas
// por más que hayan llegado todos los EOFs pueden haber descargas en curso.
type clientState struct {
	header       inner.Header
	inFlight     int  // descargas encoladas/en curso que todavia no terminaron
	eofsReceived int  // EOFs de filter recibidos
	done         bool // ya se emitio el EndOfResults(Q2) (tombstone)
}

type ImageDownloader struct {
	mom                   middleware.MultiRabbitMiddleware
	fetcher               *Fetcher
	filterAmount          int
	workers               int
	outputGatewayExchange string

	jobs chan job       // el dispatcher encola, los workers consumen
	wg   sync.WaitGroup // espera a los workers en el shutdown

	mutex   sync.Mutex // protege clients
	clients map[string]*clientState
}

func NewImageDownloader(config Config) (*ImageDownloader, error) {
	if config.FilterAmount < 1 {
		return nil, errors.New("image downloader needs FILTER_AMOUNT")
	}
	if config.OutputGatewayExchange == "" {
		return nil, errors.New("image downloader needs OUTPUT_GATEWAY_EXCHANGE")
	}
	if config.Workers < 1 {
		return nil, errors.New("image downloader needs IDOWNLOADER_WORKERS >= 1")
	}
	downloader := &ImageDownloader{
		fetcher:               NewFetcher(config.UserAgent, time.Duration(config.HTTPTimeoutSeconds)*time.Second),
		filterAmount:          config.FilterAmount,
		workers:               config.Workers,
		outputGatewayExchange: config.OutputGatewayExchange,
		jobs:                  make(chan job),
		clients:               make(map[string]*clientState),
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
		// con W workers queremos W descargas en al mismo tiempo y una extra para el EOF
		Prefetch: config.Workers + 1,
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
	downloader.startWorkers()
	err := downloader.mom.StartConsuming()
	close(downloader.jobs)
	downloader.wg.Wait() // espera las descargas en curso antes de cortar
	return err
}

func (downloader *ImageDownloader) Shutdown() {
	_ = downloader.mom.StopConsuming()
}

func (downloader *ImageDownloader) startWorkers() {
	downloader.wg.Add(downloader.workers)
	for range downloader.workers {
		go downloader.worker()
	}
}

func (downloader *ImageDownloader) worker() {
	defer downloader.wg.Done()
	for j := range downloader.jobs {
		downloader.process(j)
	}
}

// callback corre en un thread: procesa secuencialmente los mensajes recibidos
// encolando las tareas para los workers e incrementa inFlight por biografía recibida,
// obviamente también es el que procesa los EOFs
func (downloader *ImageDownloader) callback(msg middleware.Message, ack func(), nack func()) {
	data := []byte(msg.Body)
	op, err := inner.ReadOpcode(data)
	if err != nil {
		slog.Error("Leyendo opcode", "err", err)
		nack()
		return
	}

	switch op {
	case inner.OpBiography:
		header, biography, err := inner.DecodeBiography(data)
		if err != nil {
			slog.Error("Decodificando biografia", "err", err)
			nack()
			return
		}
		downloader.enqueueBiography(header, biography, ack) // el ack lo hace el worker
	case inner.OpEndOfRecords:
		header, err := inner.DecodeEndOfRecords(data)
		if err != nil {
			slog.Error("Decodificando EOF", "err", err)
			nack()
			return
		}
		ack() // el EOF se ackea al toque, desacoplado de la barrera
		downloader.onEndOfRecords(header)
	default:
		slog.Error("Opcode inesperado", "op", op)
		nack()
	}
}

// enqueueBiography pone descarga en curso y la manda al channel.
func (downloader *ImageDownloader) enqueueBiography(header inner.Header, biography inner.Biography, ack func()) {
	downloader.mutex.Lock()
	downloader.clientLocked(header).inFlight++
	downloader.mutex.Unlock()
	downloader.jobs <- job{header: header, biography: biography, ack: ack}
}

// process baja la imagen (sin lock) y publica. después actualiza la barrera.
func (downloader *ImageDownloader) process(j job) {
	if err := downloader.publishBiography(j.header, j.biography); err != nil {
		slog.Error("Publicando Q2Result", "client", j.header.ClientID, "err", err)
	}
	j.ack()
	downloader.onBiographyDone(j.header.ClientID)
}

// publishBiography baja la imagen (si hay) y publica el Q2Result. Si el articulo
// no tiene imagen se publica igual con bytes vacios, si la descarga
// falla, por ahora se saltea.
func (downloader *ImageDownloader) publishBiography(header inner.Header, biography inner.Biography) error {
	if biography.ImageURL == "" {
		slog.Info("Biografia sin imágen", "client", header.ClientID, "name", biography.Name)
		return downloader.publishResult(header, biography, nil)
	}
	imgBytes, err := downloader.fetcher.FetchImage(biography.ImageURL)
	if err != nil {
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

// onBiographyDone descuenta una descarga y chequea la barrera.
func (downloader *ImageDownloader) onBiographyDone(clientID string) {
	downloader.mutex.Lock()
	state := downloader.clients[clientID]
	state.inFlight--
	finished := downloader.barrierReachedLocked(clientID, state)
	header := state.header
	downloader.mutex.Unlock()
	if finished {
		downloader.sendEndOfResults(header)
	}
}

// onEndOfRecords suma un EOF de filter y chequea la barrera.
func (downloader *ImageDownloader) onEndOfRecords(header inner.Header) {
	downloader.mutex.Lock()
	state := downloader.clientLocked(header)
	state.eofsReceived++
	finished := downloader.barrierReachedLocked(header.ClientID, state)
	downloader.mutex.Unlock()
	if finished {
		downloader.sendEndOfResults(header)
	}
}

// barrierReachedLocked (con mutex tomado) devuelve true una sola vez por cliente:
// cuando llegaron todos los EOF Y no quedan descargas en curso. Se checkea tanto
// en el momento que se termina de procesar una biografía y cuando llega un EOF siendo
// que no sabemos cuándo se va a cumplir (race condition).
func (downloader *ImageDownloader) barrierReachedLocked(clientID string, state *clientState) bool {
	if state.done || state.eofsReceived < downloader.filterAmount || state.inFlight > 0 {
		return false
	}
	state.done = true
	delete(downloader.clients, clientID)
	return true
}

func (downloader *ImageDownloader) sendEndOfResults(header inner.Header) {
	slog.Info("Q2 terminado", "client", header.ClientID)
	body := inner.EncodeEndOfResults(header, inner.Q2)
	if err := downloader.mom.Publish(downloader.outputGatewayExchange, []string{header.GatewayID}, middleware.Message{Body: string(body)}); err != nil {
		slog.Error("Publicando EndOfResults(Q2)", "client", header.ClientID, "err", err)
	}
}

// clientLocked devuelve el estado del cliente (creandolo si no existe), necesita el mutex tomado.
func (downloader *ImageDownloader) clientLocked(header inner.Header) *clientState {
	state, ok := downloader.clients[header.ClientID]
	if !ok {
		state = &clientState{header: header}
		downloader.clients[header.ClientID] = state
	}
	return state
}
