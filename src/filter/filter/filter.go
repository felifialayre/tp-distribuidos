package filter

import (
	"log/slog"
	"time"
	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/parquet_row"
	"tp-distribuidos/common/protocol/inner"
)

// Q1: articulos en ingles con fecha de modificacion anterior a este corte
var q1Cutoff = time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)

// clientStats cuenta lo procesado de un cliente, solo para el log del EOF
type clientStats struct {
	records int //filas recibidas (para testear por ahora)
	q1      int //filas que pasaron el filtro
}

type Filter struct {
	inputQueue middleware.Middleware
	settings   middleware.ConnSettings
	outputs    map[string]middleware.Middleware //una por gateway
	stats      map[string]*clientStats
}

func NewFilter(queueName string, host string, port int) (*Filter, error) {
	middlewareSettings := middleware.ConnSettings{
		Hostname: host,
		Port:     port,
	}
	inputQueue, err := middleware.CreateQueueMiddleware(queueName, middlewareSettings)
	if err != nil {
		return nil, err
	}
	return &Filter{
		inputQueue: inputQueue,
		settings:   middlewareSettings,
		outputs:    make(map[string]middleware.Middleware),
		stats:      make(map[string]*clientStats),
	}, nil
}

func (f *Filter) Run() error {
	defer f.close()
	return f.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		if err := f.handleMessage([]byte(msg.Body)); err != nil {
			slog.Error("Handling message", "err", err)
			nack()
			return
		}
		ack()
	})
}

func (f *Filter) handleMessage(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}

	switch op {
	case inner.OpRecord:
		header, row, err := inner.DecodeRecord(data)
		if err != nil {
			return err
		}
		return f.handleRecord(header, row)
	case inner.OpEndOfRecords:
		header, err := inner.DecodeEndOfRecords(data)
		if err != nil {
			return err
		}
		return f.handleEndOfRecords(header)
	default:
		return inner.ErrUnexpectedOpcode
	}
}

func (f *Filter) handleRecord(header inner.Header, row *parquet_row.ParquetRow) error {
	stats := f.clientStats(header.ClientID)
	stats.records++

	if row.InLanguage != "en" {
		return nil
	}
	modified, err := parquet_row.ParseDate(row.DateModified)
	if err != nil {
		slog.Warn("Invalid date_modified", "client", header.ClientID, "url", row.URL, "err", err)
		return nil // la fila se descarta, no es un error del mensaje
	}
	//la clase de common nueva deberia encargarse de ver si se cumple la condicion
	// y ademas el filter podria recibir la funcion de filtro digamos
	if !modified.Before(q1Cutoff) {
		return nil
	}

	stats.q1++
	result := inner.Q1Result{URL: row.URL, DateModified: row.DateModified}
	return f.send(header.GatewayID, inner.EncodeQ1Result(header, result))
}

func (f *Filter) handleEndOfRecords(header inner.Header) error {
	stats := f.clientStats(header.ClientID)
	slog.Info("End of records", "client", header.ClientID, "records", stats.records, "q1", stats.q1)
	delete(f.stats, header.ClientID)

	return f.send(header.GatewayID, inner.EncodeEndOfResults(header, inner.Q1))
}

func (f *Filter) clientStats(clientID string) *clientStats {
	stats, ok := f.stats[clientID]
	if !ok {
		stats = &clientStats{}
		f.stats[clientID] = stats
	}
	return stats
}

// send publica en la cola de resultados del gateway
func (f *Filter) send(gatewayID string, body []byte) error {
	output, ok := f.outputs[gatewayID]
	if !ok {
		var err error
		output, err = middleware.CreateQueueMiddleware(resultsQueue(gatewayID), f.settings)
		if err != nil {
			return err
		}
		f.outputs[gatewayID] = output
	}
	return output.Send(middleware.Message{Body: string(body)})
}

func (f *Filter) close() {
	for _, output := range f.outputs {
		_ = output.Close()
	}
	_ = f.inputQueue.Close()
}

// resultsQueue es la cola de la que el gateway lee sus resultados
func resultsQueue(gatewayID string) string {
	return "results_" + gatewayID
}
