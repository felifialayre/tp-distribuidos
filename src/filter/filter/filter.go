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

type FilterConfig struct {
	InputQueue      string
	MomHost         string
	MomPort         int
	ResultsExchange string
}

type Filter struct {
	mom             middleware.MultiRabbitMiddleware
	resultsExchange string
	stats           map[string]*clientStats
}

func NewFilter(config FilterConfig) (*Filter, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}

	f := &Filter{
		mom:             mom,
		resultsExchange: config.ResultsExchange,
		stats:           make(map[string]*clientStats),
	}

	if err := mom.AddInputQueue(config.InputQueue, f.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(config.ResultsExchange, middleware.Direct); err != nil {
		mom.Close()
		return nil, err
	}
	return f, nil
}

func (f *Filter) Run() error {
	defer f.mom.Close()
	return f.mom.StartConsuming()
}

func (f *Filter) Shutdown() {
	_ = f.mom.StopConsuming()
}

func (f *Filter) callback(msg middleware.Message, ack func(), nack func()) {
	if err := f.handleMessage([]byte(msg.Body)); err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (f *Filter) handleMessage(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}

	switch op {
	case inner.OpRecord:
		header, record, err := inner.DecodeRecord(data)
		if err != nil {
			return err
		}
		return f.handleRecord(header, record)
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

func (f *Filter) handleRecord(header inner.Header, row inner.Record) error {
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

func (f *Filter) send(gatewayID string, body []byte) error {
	return f.mom.Publish(f.resultsExchange, []string{gatewayID}, middleware.Message{Body: string(body)})
}
