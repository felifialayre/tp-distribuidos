package accumulator

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"time"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/parquet_row"
	"tp-distribuidos/common/protocol/inner"
)

// cuantos pares palabra/cantidad van como maximo en un mensaje hacia un reducer
const maxCountsPerMessage = 50000

// Q5: el promedio se calcula con los abstracts anteriores a esta fecha
var cutoff = time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC)

type Config struct {
	// palabras que llegan de stopwords, su key es AccumulatorPrefix + AccumulatorID
	InputStopwordsExchange string
	AccumulatorPrefix      string
	AccumulatorID          int

	// cuantas replicas de stopwords mandan palabras, cada una manda su propio EndOfRecords
	StopwordsAmount int

	// conteos a los reducers, keys: ReducerPrefix + ID (recorriendo ReducerAmount)
	OutputReducerExchange string
	ReducerPrefix         string
	ReducerAmount         int

	// suma parcial para el promedio de Q5, keys: JoinAveragePrefix + ID (elegido por cliente)
	OutputJoinAverageExchange string
	JoinAveragePrefix         string
	JoinAverageAmount         int

	MomHost string
	MomPort int
}

type clientState struct {
	counts         map[string]uint32    // palabra en cuantos abstracts aparece
	partialAverage inner.PartialAverage // palabras y abstracts anteriores a 2021 (Q5)
	eofReceived    int
}

type Accumulator struct {
	mom                 middleware.MultiRabbitMiddleware
	stopwordsAmount     int
	outputExchange      string
	reducerKeys         []string
	joinAverageExchange string
	joinAverageKeys     []string
	clients             map[string]*clientState
}

func NewAccumulator(config Config) (*Accumulator, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	a := &Accumulator{
		stopwordsAmount:     config.StopwordsAmount,
		outputExchange:      config.OutputReducerExchange,
		reducerKeys:         keys(config.ReducerPrefix, config.ReducerAmount),
		joinAverageExchange: config.OutputJoinAverageExchange,
		joinAverageKeys:     keys(config.JoinAveragePrefix, config.JoinAverageAmount),
		clients:             make(map[string]*clientState),
	}
	mom, err := a.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	a.mom = mom
	return a, nil
}

func validateConfig(config Config) error {
	if config.StopwordsAmount < 1 {
		return errors.New("accumulator needs STOPWORDS_AMOUNT")
	}
	if config.OutputReducerExchange == "" || config.ReducerPrefix == "" || config.ReducerAmount < 1 {
		return errors.New("accumulator needs OUTPUT_REDUCER_EXCHANGE, REDUCER_PREFIX and REDUCER_AMOUNT")
	}
	if config.OutputJoinAverageExchange == "" || config.JoinAveragePrefix == "" || config.JoinAverageAmount < 1 {
		return errors.New("accumulator needs OUTPUT_JOIN_AVERAGE_EXCHANGE, JOIN_AVERAGE_PREFIX and JOIN_AVERAGE_AMOUNT")
	}
	return nil
}

func (a *Accumulator) setUpRabbitCommunication(config Config) (middleware.MultiRabbitMiddleware, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s_%d", config.AccumulatorPrefix, config.AccumulatorID)
	if err := mom.AddInputExchange(config.InputStopwordsExchange, middleware.Direct, []string{key}, a.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(a.outputExchange, middleware.Direct); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(a.joinAverageExchange, middleware.Direct); err != nil {
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

func (a *Accumulator) Run() error {
	defer a.mom.Close()
	return a.mom.StartConsuming()
}

func (a *Accumulator) Shutdown() {
	_ = a.mom.StopConsuming()
}

func (a *Accumulator) callback(msg middleware.Message, ack func(), nack func()) {
	if err := a.handleMessage([]byte(msg.Body)); err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (a *Accumulator) handleMessage(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}

	switch op {
	case inner.OpWords:
		header, words, err := inner.DecodeWords(data)
		if err != nil {
			return err
		}
		a.handleWords(header, words)
		return nil
	case inner.OpEndOfRecords:
		header, err := inner.DecodeEndOfRecords(data)
		if err != nil {
			return err
		}
		return a.handleEndOfRecords(header)
	default:
		return inner.ErrUnexpectedOpcode
	}
}

// las palabras de un abstract ya vienen sin repetidos, asi que cada una suma un abstract
func (a *Accumulator) handleWords(header inner.Header, words inner.Words) {
	state := a.client(header.ClientID)
	for _, word := range words.Words {
		state.counts[word]++
	}

	modified, err := parquet_row.ParseDate(words.DateModified)
	if err == nil && modified.Before(cutoff) { // la fecha invalida ya la avisa q1
		state.partialAverage.Words += uint64(len(words.Words))
		state.partialAverage.Abstracts++
	}
}

func (a *Accumulator) handleEndOfRecords(header inner.Header) error {
	state := a.client(header.ClientID)
	state.eofReceived++
	if state.eofReceived < a.stopwordsAmount {
		return nil
	}
	delete(a.clients, header.ClientID)
	slog.Info("End of words", "client", header.ClientID, "words", len(state.counts))

	if err := a.sendCounts(header, state.counts); err != nil {
		return err
	}

	eof := middleware.Message{Body: string(inner.EncodeEndOfRecords(header))}
	if err := a.mom.Publish(a.outputExchange, a.reducerKeys, eof); err != nil {
		return err
	}

	// todos los parciales de un cliente van al mismo join average; el parcial hace de EOF
	key := a.joinAverageKeys[route(header.ClientID, len(a.joinAverageKeys))]
	body := inner.EncodePartialAverage(header, state.partialAverage)
	return a.mom.Publish(a.joinAverageExchange, []string{key}, middleware.Message{Body: string(body)})
}

// la misma palabra de un cliente va siempre al mismo reducer, venga del accumulator que venga
func (a *Accumulator) sendCounts(header inner.Header, counts map[string]uint32) error {
	byReducer := make([][]inner.WordCount, len(a.reducerKeys))
	for word, count := range counts {
		i := route(header.ClientID+"|"+word, len(a.reducerKeys))
		byReducer[i] = append(byReducer[i], inner.WordCount{Word: word, Count: count})
	}
	for i, list := range byReducer {
		for len(list) > 0 {
			chunk := list[:min(len(list), maxCountsPerMessage)]
			list = list[len(chunk):]
			body := inner.EncodeWordCounts(header, chunk)
			if err := a.mom.Publish(a.outputExchange, []string{a.reducerKeys[i]}, middleware.Message{Body: string(body)}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Accumulator) client(clientID string) *clientState {
	state, ok := a.clients[clientID]
	if !ok {
		state = &clientState{counts: make(map[string]uint32)}
		a.clients[clientID] = state
	}
	return state
}

func route(hashKey string, amount int) int {
	return int(crc32.ChecksumIEEE([]byte(hashKey)) % uint32(amount))
}
