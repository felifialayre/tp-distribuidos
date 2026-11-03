package above_average

import (
	"errors"
	"fmt"
	"log/slog"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
)

// cuantos articulos van como maximo en un mensaje de resultado hacia el gateway
const maxArticlesPerMessage = 1000

type Config struct {
	// articulos de 2021 en adelante que llegan de stopwords y promedio que llega del join average,
	// en los dos su key es AboveAveragePrefix + AboveAverageID
	InputStopwordsExchange string
	InputAverageExchange   string
	AboveAveragePrefix     string
	AboveAverageID         int

	// cuantas replicas de stopwords mandan articulos, cada una manda EOF
	StopwordsAmount int

	// resultado de Q5, directo al gateway
	OutputGatewayExchange string

	MomHost string
	MomPort int
}

type clientState struct {
	articles    []inner.Q5Article // por ahora en memoria
	average     *inner.Average    // nil hasta que llega del join average
	eofReceived int
}

type AboveAverageFilter struct {
	mom                   middleware.MultiRabbitMiddleware
	stopwordsAmount       int
	outputGatewayExchange string
	clients               map[string]*clientState
}

func NewAboveAverageFilter(config Config) (*AboveAverageFilter, error) {
	if config.StopwordsAmount < 1 {
		return nil, errors.New("above average filter needs STOPWORDS_AMOUNT")
	}
	if config.OutputGatewayExchange == "" {
		return nil, errors.New("above average filter needs OUTPUT_GATEWAY_EXCHANGE")
	}
	f := &AboveAverageFilter{
		stopwordsAmount:       config.StopwordsAmount,
		outputGatewayExchange: config.OutputGatewayExchange,
		clients:               make(map[string]*clientState),
	}
	mom, err := f.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	f.mom = mom
	return f, nil
}

func (f *AboveAverageFilter) setUpRabbitCommunication(config Config) (middleware.MultiRabbitMiddleware, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s_%d", config.AboveAveragePrefix, config.AboveAverageID)
	if err := mom.AddInputExchange(config.InputStopwordsExchange, middleware.Direct, []string{key}, f.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddInputExchange(config.InputAverageExchange, middleware.Direct, []string{key}, f.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(config.OutputGatewayExchange, middleware.Direct); err != nil {
		mom.Close()
		return nil, err
	}
	return mom, nil
}

func (f *AboveAverageFilter) Run() error {
	defer f.mom.Close()
	return f.mom.StartConsuming()
}

func (f *AboveAverageFilter) Shutdown() {
	_ = f.mom.StopConsuming()
}

func (f *AboveAverageFilter) callback(msg middleware.Message, ack func(), nack func()) {
	if err := f.handleMessage([]byte(msg.Body)); err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (f *AboveAverageFilter) handleMessage(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}

	switch op {
	case inner.OpQ5Article:
		header, article, err := inner.DecodeQ5Article(data)
		if err != nil {
			return err
		}
		state := f.client(header.ClientID)
		state.articles = append(state.articles, article)
		return nil
	case inner.OpEndOfRecords:
		header, err := inner.DecodeEndOfRecords(data)
		if err != nil {
			return err
		}
		f.client(header.ClientID).eofReceived++
		return f.tryFilter(header)
	case inner.OpAverage:
		header, average, err := inner.DecodeAverage(data)
		if err != nil {
			return err
		}
		f.client(header.ClientID).average = &average
		return f.tryFilter(header)
	default:
		return inner.ErrUnexpectedOpcode
	}
}

// filtra recien cuando tiene los articulos de todas las replicas de stopwords y el promedio,
// lleguen en el orden que lleguen
func (f *AboveAverageFilter) tryFilter(header inner.Header) error {
	state := f.client(header.ClientID)
	if state.eofReceived < f.stopwordsAmount || state.average == nil {
		return nil
	}
	delete(f.clients, header.ClientID)

	var above []inner.Q5Article
	for _, article := range state.articles {
		if float64(article.Count) > state.average.Value {
			above = append(above, article)
		}
	}
	slog.Info("Q5 filtered", "client", header.ClientID, "articles", len(state.articles), "above", len(above))

	for len(above) > 0 {
		chunk := above[:min(len(above), maxArticlesPerMessage)]
		above = above[len(chunk):]
		if err := f.send(header.GatewayID, inner.EncodeQ5Result(header, chunk)); err != nil {
			return err
		}
	}
	return f.send(header.GatewayID, inner.EncodeEndOfResults(header, inner.Q5))
}

func (f *AboveAverageFilter) client(clientID string) *clientState {
	state, ok := f.clients[clientID]
	if !ok {
		state = &clientState{}
		f.clients[clientID] = state
	}
	return state
}

func (f *AboveAverageFilter) send(gatewayID string, body []byte) error {
	return f.mom.Publish(f.outputGatewayExchange, []string{gatewayID}, middleware.Message{Body: string(body)})
}
