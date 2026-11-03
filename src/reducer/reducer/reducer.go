package reducer

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
	"tp-distribuidos/common/ranking"
)

type Config struct {
	// conteos que llegan de los accumulators, su key es ReducerPrefix + ReducerID
	InputAccumulatorExchange string
	ReducerPrefix            string
	ReducerID                int

	// cuantos accumulators (replicas) mandan conteos, cada uno manda su propio EndOfRecords
	AccumulatorAmount int

	// top parcial al sorter, keys: SorterPrefix + ID (recorriendo SorterAmount)
	OutputSorterExchange string
	SorterPrefix         string
	SorterAmount         int

	MomHost string
	MomPort int
}

type clientState struct {
	counts      map[string]uint32
	eofReceived int
}

type Reducer struct {
	mom               middleware.MultiRabbitMiddleware
	accumulatorAmount int
	outputExchange    string
	sorterKeys        []string
	clients           map[string]*clientState
}

func NewReducer(config Config) (*Reducer, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	r := &Reducer{
		accumulatorAmount: config.AccumulatorAmount,
		outputExchange:    config.OutputSorterExchange,
		sorterKeys:        keys(config.SorterPrefix, config.SorterAmount),
		clients:           make(map[string]*clientState),
	}
	mom, err := r.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	r.mom = mom
	return r, nil
}

func validateConfig(config Config) error {
	if config.AccumulatorAmount < 1 {
		return errors.New("reducer needs ACCUMULATOR_AMOUNT")
	}
	if config.OutputSorterExchange == "" || config.SorterPrefix == "" || config.SorterAmount < 1 {
		return errors.New("reducer needs OUTPUT_SORTER_EXCHANGE, SORTER_PREFIX and SORTER_AMOUNT")
	}
	return nil
}

func (r *Reducer) setUpRabbitCommunication(config Config) (middleware.MultiRabbitMiddleware, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s_%d", config.ReducerPrefix, config.ReducerID)
	if err := mom.AddInputExchange(config.InputAccumulatorExchange, middleware.Direct, []string{key}, r.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(r.outputExchange, middleware.Direct); err != nil {
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

func (r *Reducer) Run() error {
	defer r.mom.Close()
	return r.mom.StartConsuming()
}

func (r *Reducer) Shutdown() {
	_ = r.mom.StopConsuming()
}

func (r *Reducer) callback(msg middleware.Message, ack func(), nack func()) {
	if err := r.handleMessage([]byte(msg.Body)); err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (r *Reducer) handleMessage(data []byte) error {
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
		r.handleWordCounts(header, counts)
		return nil
	case inner.OpEndOfRecords:
		header, err := inner.DecodeEndOfRecords(data)
		if err != nil {
			return err
		}
		return r.handleEndOfRecords(header)
	default:
		return inner.ErrUnexpectedOpcode
	}
}

func (r *Reducer) handleWordCounts(header inner.Header, counts []inner.WordCount) {
	state := r.client(header.ClientID)
	for _, wc := range counts {
		state.counts[wc.Word] += wc.Count
	}
}

func (r *Reducer) handleEndOfRecords(header inner.Header) error {
	state := r.client(header.ClientID)
	state.eofReceived++
	if state.eofReceived < r.accumulatorAmount {
		return nil
	}
	delete(r.clients, header.ClientID)

	top := ranking.NewTopN(ranking.TopSize)
	for word, count := range state.counts {
		top.Add(inner.WordCount{Word: word, Count: count})
	}
	slog.Info("Partial top ready", "client", header.ClientID, "words", len(state.counts))

	key := []string{r.sorterKeys[crc32.ChecksumIEEE([]byte(header.ClientID))%uint32(len(r.sorterKeys))]}
	if items := top.Items(); len(items) > 0 {
		body := inner.EncodeWordCounts(header, items)
		if err := r.mom.Publish(r.outputExchange, key, middleware.Message{Body: string(body)}); err != nil {
			return err
		}
	}
	eof := middleware.Message{Body: string(inner.EncodeEndOfRecords(header))}
	return r.mom.Publish(r.outputExchange, key, eof)
}

func (r *Reducer) client(clientID string) *clientState {
	state, ok := r.clients[clientID]
	if !ok {
		state = &clientState{counts: make(map[string]uint32)}
		r.clients[clientID] = state
	}
	return state
}
