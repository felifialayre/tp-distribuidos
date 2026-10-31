package stopwords

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
)

type Config struct {
	// abstracts que llegan de los filters; su key es StopwordsPrefix + StopwordsID
	InputFilterExchange string
	StopwordsPrefix     string
	StopwordsID         int

	// cuantos filters (replicas) mandan abstracts: cada uno manda su propio EndOfRecords
	FilterAmount int

	// palabras con idioma al join, keys: JoinPrefix + ID (recorriendo JoinAmount)
	OutputJoinExchange string
	JoinPrefix         string
	JoinAmount         int

	MomHost string
	MomPort int
}

type StopwordsFilter struct {
	mom                middleware.MultiRabbitMiddleware
	filterAmount       int
	outputJoinExchange string
	outputJoinKeys     []string
	endsReceived       map[string]int //por cliente
}

func NewStopwordsFilter(config Config) (*StopwordsFilter, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	s := &StopwordsFilter{
		filterAmount:       config.FilterAmount,
		outputJoinExchange: config.OutputJoinExchange,
		outputJoinKeys:     keys(config.JoinPrefix, config.JoinAmount),
		endsReceived:       make(map[string]int),
	}
	mom, err := s.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	s.mom = mom
	return s, nil
}

func validateConfig(config Config) error {
	if config.FilterAmount < 1 {
		return errors.New("stopwords needs FILTER_AMOUNT")
	}
	if config.OutputJoinExchange == "" || config.JoinPrefix == "" || config.JoinAmount < 1 {
		return errors.New("stopwords needs OUTPUT_JOIN_EXCHANGE, JOIN_PREFIX and JOIN_AMOUNT")
	}
	return nil
}

func (s *StopwordsFilter) setUpRabbitCommunication(config Config) (middleware.MultiRabbitMiddleware, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s_%d", config.StopwordsPrefix, config.StopwordsID)
	if err := mom.AddInputExchange(config.InputFilterExchange, middleware.Direct, []string{key}, s.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(s.outputJoinExchange, middleware.Direct); err != nil {
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

func (s *StopwordsFilter) Run() error {
	defer s.mom.Close()
	return s.mom.StartConsuming()
}

func (s *StopwordsFilter) Shutdown() {
	_ = s.mom.StopConsuming()
}

func (s *StopwordsFilter) callback(msg middleware.Message, ack func(), nack func()) {
	if err := s.handleMessage([]byte(msg.Body)); err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (s *StopwordsFilter) handleMessage(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}

	switch op {
	case inner.OpAbstract:
		header, abstract, err := inner.DecodeAbstract(data)
		if err != nil {
			return err
		}
		return s.handleAbstract(header, abstract)
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

func (s *StopwordsFilter) handleAbstract(header inner.Header, abstract inner.Abstract) error {
	result := words(abstract.Text)
	//todo: habria que analizar mejor el caso de que tenga len(result)=0
	//if len(result) == 0 {
	//	return nil
	//}
	if err := s.sendQ5(header, abstract, len(result)); err != nil {
		return err
	}

	if err := s.sendQ4(header, abstract, result); err != nil {
		return err
	}
	return s.sendToJoin(header, abstract, result)
}

// pendiente Q5
func (s *StopwordsFilter) sendQ5(header inner.Header, abstract inner.Abstract, count int) error {
	return nil
}

// pendiente Q4
func (s *StopwordsFilter) sendQ4(header inner.Header, abstract inner.Abstract, result []string) error {
	return nil
}

// la misma palabra de un cliente va siempre a la misma replica del join,
// asi que las palabras se agrupan por replica y se manda un mensaje a cada una
func (s *StopwordsFilter) sendToJoin(header inner.Header, abstract inner.Abstract, result []string) error {
	byKey := make(map[string][]string)
	for _, word := range result {
		key := route(header.ClientID+"|"+word, s.outputJoinKeys)
		byKey[key] = append(byKey[key], word)
	}
	for key, group := range byKey {
		body := inner.EncodeLangWords(header, inner.LangWords{Words: group, Lang: abstract.Lang})
		if err := s.mom.Publish(s.outputJoinExchange, []string{key}, middleware.Message{Body: string(body)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *StopwordsFilter) handleEndOfRecords(header inner.Header) error {
	s.endsReceived[header.ClientID]++
	if s.endsReceived[header.ClientID] < s.filterAmount {
		return nil // faltan filters por terminar
	}
	delete(s.endsReceived, header.ClientID)
	slog.Info("End of abstracts", "client", header.ClientID)

	// le aviso a todas las replicas del join
	eof := middleware.Message{Body: string(inner.EncodeEndOfRecords(header))}
	return s.mom.Publish(s.outputJoinExchange, s.outputJoinKeys, eof)
}

func route(hashKey string, keys []string) string {
	return keys[crc32.ChecksumIEEE([]byte(hashKey))%uint32(len(keys))]
}
