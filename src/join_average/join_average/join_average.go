package join_average

import (
	"errors"
	"fmt"
	"log/slog"
	"math"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
)

type Config struct {
	// sumas parciales que llegan de los accumulators, su key es JoinAveragePrefix + JoinAverageID
	InputAccumulatorExchange string
	JoinAveragePrefix        string
	JoinAverageID            int

	// cuantos accumulators (replicas) mandan su parcial, uno por cliente cada uno
	AccumulatorAmount int

	// promedio a todos los above average filters, keys: AboveAveragePrefix + ID (recorriendo AboveAverageAmount)
	OutputAboveAverageExchange string
	AboveAveragePrefix         string
	AboveAverageAmount         int

	MomHost string
	MomPort int
}

type clientState struct {
	words            uint64 // suma de los parciales
	abstracts        uint64
	partialsReceived int
}

type JoinAverage struct {
	mom               middleware.MultiRabbitMiddleware
	accumulatorAmount int
	outputExchange    string
	aboveAverageKeys  []string
	clients           map[string]*clientState
}

func NewJoinAverage(config Config) (*JoinAverage, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	j := &JoinAverage{
		accumulatorAmount: config.AccumulatorAmount,
		outputExchange:    config.OutputAboveAverageExchange,
		aboveAverageKeys:  keys(config.AboveAveragePrefix, config.AboveAverageAmount),
		clients:           make(map[string]*clientState),
	}
	mom, err := j.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	j.mom = mom
	return j, nil
}

func validateConfig(config Config) error {
	if config.AccumulatorAmount < 1 {
		return errors.New("join average needs ACCUMULATOR_AMOUNT")
	}
	if config.OutputAboveAverageExchange == "" || config.AboveAveragePrefix == "" || config.AboveAverageAmount < 1 {
		return errors.New("join average needs OUTPUT_ABOVE_AVERAGE_EXCHANGE, ABOVE_AVERAGE_PREFIX and ABOVE_AVERAGE_AMOUNT")
	}
	return nil
}

func (j *JoinAverage) setUpRabbitCommunication(config Config) (middleware.MultiRabbitMiddleware, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s_%d", config.JoinAveragePrefix, config.JoinAverageID)
	if err := mom.AddInputExchange(config.InputAccumulatorExchange, middleware.Direct, []string{key}, j.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(j.outputExchange, middleware.Direct); err != nil {
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

func (j *JoinAverage) Run() error {
	defer j.mom.Close()
	return j.mom.StartConsuming()
}

func (j *JoinAverage) Shutdown() {
	_ = j.mom.StopConsuming()
}

func (j *JoinAverage) callback(msg middleware.Message, ack func(), nack func()) {
	if err := j.handleMessage([]byte(msg.Body)); err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (j *JoinAverage) handleMessage(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}
	if op != inner.OpPartialAverage {
		return inner.ErrUnexpectedOpcode
	}
	header, partial, err := inner.DecodePartialAverage(data)
	if err != nil {
		return err
	}
	return j.handlePartialAverage(header, partial)
}

// suma el parcial de un accumulator; con los de todos manda el total a los above average filters
func (j *JoinAverage) handlePartialAverage(header inner.Header, partial inner.PartialAverage) error {
	state, ok := j.clients[header.ClientID]
	if !ok {
		state = &clientState{}
		j.clients[header.ClientID] = state
	}
	state.words += partial.Words
	state.abstracts += partial.Abstracts
	state.partialsReceived++
	if state.partialsReceived < j.accumulatorAmount {
		return nil // faltan accumulators por terminar
	}
	delete(j.clients, header.ClientID)
	average := inner.Average{Value: math.Inf(1)} // sin abstracts anteriores a 2021 ningun articulo lo supera
	if state.abstracts > 0 {
		average.Value = float64(state.words) / float64(state.abstracts)
	}
	slog.Info("Average ready", "client", header.ClientID, "average", average.Value, "abstracts", state.abstracts)

	body := inner.EncodeAverage(header, average)
	return j.mom.Publish(j.outputExchange, j.aboveAverageKeys, middleware.Message{Body: string(body)})
}
