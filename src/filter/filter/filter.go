package filter

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"strings"
	"time"
	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/parquet_row"
	"tp-distribuidos/common/protocol/inner"
)

// condicion fecha q1 y q2
var cutoff = time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)

type FilterConfig struct {
	// filas que llegan del gateway; su key es FilterPrefix + FilterID
	InputGatewayExchange string
	FilterPrefix         string
	FilterID             int

	// resultado de Q1, directo al gateway
	OutputGatewayExchange string

	// Q345 al stopwords filter, keys: StopwordsPrefix + ID (recorriendo StopwordsAmount)
	OutputStopwordsExchange string
	StopwordsPrefix         string
	StopwordsAmount         int

	// Q2 al image downloader, keys: Q2Prefix + ID (recorriendo Q2Amount)
	OutputIDownloaderExchange string
	Q2Prefix                  string
	Q2Amount                  int

	// datos para conexion con rabbit
	MomHost string
	MomPort int
}

type Filter struct {
	mom                       middleware.MultiRabbitMiddleware
	outputStopwordsExchange   string
	outputStopwordsKeys       []string // una clave de ruteo por replica de stopwords
	outputIDownloaderExchange string
	outputIDownloaderKeys     []string // una clave de ruteo por replica de image downloader
	outputGatewayExchange     string
}

func NewFilter(config FilterConfig) (*Filter, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	f := &Filter{
		outputStopwordsExchange:   config.OutputStopwordsExchange,
		outputStopwordsKeys:       keys(config.StopwordsPrefix, config.StopwordsAmount),
		outputIDownloaderExchange: config.OutputIDownloaderExchange,
		outputIDownloaderKeys:     keys(config.Q2Prefix, config.Q2Amount),
		outputGatewayExchange:     config.OutputGatewayExchange,
	}
	mom, err := f.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	f.mom = mom
	return f, nil
}

func validateConfig(config FilterConfig) error {
	if config.OutputStopwordsExchange == "" || config.StopwordsPrefix == "" || config.StopwordsAmount < 1 {
		return errors.New("filter needs OUTPUT_STOPWORDS_EXCHANGE, STOPWORDS_PREFIX and STOPWORDS_AMOUNT")
	}
	if config.OutputIDownloaderExchange == "" || config.Q2Prefix == "" || config.Q2Amount < 1 {
		return errors.New("filter needs OUTPUT_IDOWNLOADER_EXCHANGE, IDOWNLOADER_PREFIX and IDOWNLOADER_AMOUNT")
	}
	return nil
}

func (f *Filter) setUpRabbitCommunication(config FilterConfig) (middleware.MultiRabbitMiddleware, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}
	if err := mom.AddInputExchange(config.InputGatewayExchange, middleware.Direct, []string{fmt.Sprintf("%s_%d", config.FilterPrefix, config.FilterID)}, f.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(config.OutputGatewayExchange, middleware.Direct); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(config.OutputStopwordsExchange, middleware.Direct); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(config.OutputIDownloaderExchange, middleware.Direct); err != nil {
		mom.Close()
		return nil, err
	}
	return mom, nil
}

func keys(prefix string, amount int) []string {
	listOfKeys := make([]string, 0, amount)
	for i := range amount {
		listOfKeys = append(listOfKeys, fmt.Sprintf("%s_%d", prefix, i))
	}
	return listOfKeys
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
	if err := f.q1(header, row); err != nil {
		return err
	}
	if err := f.q2(header, row); err != nil {
		return err
	}
	return f.q345(header, row)
}

func (f *Filter) q1(header inner.Header, row inner.Record) error {
	if row.InLanguage != "en" {
		return nil
	}
	modified, err := parquet_row.ParseDate(row.DateModified)
	if err != nil {
		slog.Warn("Invalid date_modified", "client", header.ClientID, "url", row.URL, "err", err)
		return nil // la fila se descarta, no es un error del mensaje
	}
	if !modified.Before(cutoff) {
		return nil
	}

	result := inner.Q1Result{URL: row.URL, DateModified: row.DateModified}
	return f.send(header.GatewayID, inner.EncodeQ1Result(header, result))
}

func (f *Filter) q2(header inner.Header, row inner.Record) error {
	modified, err := parquet_row.ParseDate(row.DateModified)
	if err != nil || !modified.Before(cutoff) { // la fecha invalida ya la avisa q1
		return nil
	}
	if !hasBiography(row.Sections) {
		return nil
	}

	bio := inner.Biography{Name: row.Name, URL: row.URL, Sections: row.Sections, ImageURL: row.Image}
	key := f.outputIDownloaderKeys[crc32.ChecksumIEEE([]byte(row.URL))%uint32(len(f.outputIDownloaderKeys))]
	return f.mom.Publish(f.outputIDownloaderExchange, []string{key}, middleware.Message{Body: string(inner.EncodeBiography(header, bio))})
}

func hasBiography(sections string) bool {
	var parsed []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(sections), &parsed); err != nil {
		return false
	}
	for _, section := range parsed {
		name := strings.ToLower(section.Name)
		if name == "biography" || name == "biographie" {
			return true
		}
	}
	return false
}

func (f *Filter) q345(header inner.Header, row inner.Record) error {
	abs := inner.Abstract{URL: row.URL, Lang: row.InLanguage, DateModified: row.DateModified, Text: row.Abstract}
	// la misma url va siempre a la misma replica
	key := f.outputStopwordsKeys[crc32.ChecksumIEEE([]byte(row.URL))%uint32(len(f.outputStopwordsKeys))]
	return f.mom.Publish(f.outputStopwordsExchange, []string{key}, middleware.Message{Body: string(inner.EncodeAbstract(header, abs))})
}

func (f *Filter) handleEndOfRecords(header inner.Header) error {
	//manda EOF al gateway (flujo q1)
	if err := f.send(header.GatewayID, inner.EncodeEndOfResults(header, inner.Q1)); err != nil {
		return err
	}

	eof := inner.EncodeEndOfRecords(header)

	// manda EOF al image downloader (flujo q2)
	if err := f.mom.Publish(f.outputIDownloaderExchange, f.outputIDownloaderKeys, middleware.Message{Body: string(eof)}); err != nil {
		return err
	}

	// manda EOF al stopWords (flujo q345)
	return f.mom.Publish(f.outputStopwordsExchange, f.outputStopwordsKeys, middleware.Message{Body: string(eof)})
}

func (f *Filter) send(gatewayID string, body []byte) error {
	return f.mom.Publish(f.outputGatewayExchange, []string{gatewayID}, middleware.Message{Body: string(body)})
}
