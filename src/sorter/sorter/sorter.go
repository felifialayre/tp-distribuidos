package sorter

import (
	"errors"
	"fmt"
	"log/slog"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
	"tp-distribuidos/common/ranking"
)

type Config struct {
	// tops parciales que llegan de los reducers, su key es SorterPrefix + SorterID
	InputReducerExchange string
	SorterPrefix         string
	SorterID             int

	// cuantos reducers (replicas) mandan su top, cada uno manda su propio EndOfRecords
	ReducerAmount int

	// resultado de Q4, directo al gateway
	OutputGatewayExchange string

	MomHost string
	MomPort int
}

type clientState struct {
	top         *ranking.TopN
	eofReceived int
}

type Sorter struct {
	mom           middleware.MultiRabbitMiddleware
	reducerAmount int
	outputGateway string
	clients       map[string]*clientState
}

func NewSorter(config Config) (*Sorter, error) {
	if config.ReducerAmount < 1 {
		return nil, errors.New("sorter needs REDUCER_AMOUNT")
	}
	if config.OutputGatewayExchange == "" {
		return nil, errors.New("sorter needs OUTPUT_GATEWAY_EXCHANGE")
	}
	s := &Sorter{
		reducerAmount: config.ReducerAmount,
		outputGateway: config.OutputGatewayExchange,
		clients:       make(map[string]*clientState),
	}
	mom, err := s.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	s.mom = mom
	return s, nil
}

func (s *Sorter) setUpRabbitCommunication(config Config) (middleware.MultiRabbitMiddleware, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s_%d", config.SorterPrefix, config.SorterID)
	if err := mom.AddInputExchange(config.InputReducerExchange, middleware.Direct, []string{key}, s.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(config.OutputGatewayExchange, middleware.Direct); err != nil {
		mom.Close()
		return nil, err
	}
	return mom, nil
}

func (s *Sorter) Run() error {
	defer s.mom.Close()
	return s.mom.StartConsuming()
}

func (s *Sorter) Shutdown() {
	_ = s.mom.StopConsuming()
}

func (s *Sorter) callback(msg middleware.Message, ack func(), nack func()) {
	if err := s.handleMessage([]byte(msg.Body)); err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (s *Sorter) handleMessage(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}

	switch op {
	case inner.OpWordCounts:
		header, counts, err := inner.DecodeWordCounts(data)
		if err != nil {
			return err
		}
		s.handleWordCounts(header, counts)
		return nil
	case inner.OpEndOfRecords:
		header, err := inner.DecodeEndOfRecords(data)
		if err != nil {
			return err
		}
		return s.handleEndOfRecords(header)
	default:
		return inner.ErrUnexpectedOpcode
	}
}

func (s *Sorter) handleWordCounts(header inner.Header, counts []inner.WordCount) {
	state := s.client(header.ClientID)
	for _, wc := range counts {
		state.top.Add(wc)
	}
}

func (s *Sorter) handleEndOfRecords(header inner.Header) error {
	state := s.client(header.ClientID)
	state.eofReceived++
	if state.eofReceived < s.reducerAmount {
		return nil
	}
	delete(s.clients, header.ClientID)
	slog.Info("Top 20 ready", "client", header.ClientID)

	if items := state.top.Items(); len(items) > 0 {
		if err := s.send(header.GatewayID, inner.EncodeQ4Result(header, items)); err != nil {
			return err
		}
	}
	return s.send(header.GatewayID, inner.EncodeEndOfResults(header, inner.Q4))
}

func (s *Sorter) client(clientID string) *clientState {
	state, ok := s.clients[clientID]
	if !ok {
		state = &clientState{top: ranking.NewTopN(ranking.TopSize)}
		s.clients[clientID] = state
	}
	return state
}

func (s *Sorter) send(gatewayID string, body []byte) error {
	return s.mom.Publish(s.outputGateway, []string{gatewayID}, middleware.Message{Body: string(body)})
}
