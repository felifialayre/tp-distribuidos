package join

import (
	"errors"
	"fmt"
	"log/slog"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
)

type Config struct {
	// palabras que llegan de stopwords; su key es JoinPrefix + JoinID
	InputStopwordsExchange string
	JoinPrefix             string
	JoinID                 int

	// cuantas replicas de stopwords mandan palabras: cada una manda su propio EndOfRecords
	StopwordsAmount int

	// resultado de Q3, directo al gateway
	OutputGatewayExchange string

	MomHost string
	MomPort int
}

// idiomas en los que ya se vio una palabra
type languages struct {
	en   bool
	fr   bool
	sent bool // si ya se mando
}

type clientState struct {
	words       map[string]languages
	eofReceived int
}

type Join struct {
	mom                   middleware.MultiRabbitMiddleware
	stopwordsAmount       int
	outputGatewayExchange string
	clients               map[string]*clientState
}

func NewJoin(config Config) (*Join, error) {
	if config.StopwordsAmount < 1 {
		return nil, errors.New("join needs STOPWORDS_AMOUNT")
	}
	if config.OutputGatewayExchange == "" {
		return nil, errors.New("join needs OUTPUT_GATEWAY_EXCHANGE")
	}
	j := &Join{
		stopwordsAmount:       config.StopwordsAmount,
		outputGatewayExchange: config.OutputGatewayExchange,
		clients:               make(map[string]*clientState),
	}
	mom, err := j.setUpRabbitCommunication(config)
	if err != nil {
		return nil, err
	}
	j.mom = mom
	return j, nil
}

func (j *Join) setUpRabbitCommunication(config Config) (middleware.MultiRabbitMiddleware, error) {
	mom, err := middleware.CreateMultiRabbitMiddleware(middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	})
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s_%d", config.JoinPrefix, config.JoinID)
	if err := mom.AddInputExchange(config.InputStopwordsExchange, middleware.Direct, []string{key}, j.callback); err != nil {
		mom.Close()
		return nil, err
	}
	if err := mom.AddOutputExchange(config.OutputGatewayExchange, middleware.Direct); err != nil {
		mom.Close()
		return nil, err
	}
	return mom, nil
}

func (j *Join) Run() error {
	defer j.mom.Close()
	return j.mom.StartConsuming()
}

func (j *Join) Shutdown() {
	_ = j.mom.StopConsuming()
}

func (j *Join) callback(msg middleware.Message, ack func(), nack func()) {
	if err := j.handleMessage([]byte(msg.Body)); err != nil {
		slog.Error("Handling message", "err", err)
		nack()
		return
	}
	ack()
}

func (j *Join) handleMessage(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}

	switch op {
	case inner.OpLangWords:
		header, langWords, err := inner.DecodeLangWords(data)
		if err != nil {
			return err
		}
		return j.handleLangWords(header, langWords)
	case inner.OpEndOfRecords:
		header, err := inner.DecodeEndOfRecords(data)
		if err != nil {
			return err
		}
		return j.handleEndOfRecords(header)
	default:
		return inner.ErrUnexpectedOpcode
	}
}

// handleLangWords anota el idioma de cada palabra y manda las que ya aparecieron en ingles y en frances
func (j *Join) handleLangWords(header inner.Header, langWords inner.LangWords) error {
	if langWords.Lang != "en" && langWords.Lang != "fr" {
		return nil // Q3 solo compara estos dos idiomas
	}
	state := j.client(header.ClientID)

	var toSend []string
	for _, word := range langWords.Words {
		seen := state.words[word]
		if langWords.Lang == "en" {
			seen.en = true
		} else {
			seen.fr = true
		}
		if seen.en && seen.fr && !seen.sent {
			seen.sent = true
			toSend = append(toSend, word)
		}
		state.words[word] = seen
	}
	if len(toSend) == 0 {
		return nil
	}
	return j.send(header.GatewayID, inner.EncodeQ3Result(header, inner.Q3Result{Words: toSend}))
}

func (j *Join) handleEndOfRecords(header inner.Header) error {
	state := j.client(header.ClientID)
	state.eofReceived++
	if state.eofReceived < j.stopwordsAmount {
		return nil // faltan replicas de stopwords por terminar
	}
	delete(j.clients, header.ClientID)
	return j.send(header.GatewayID, inner.EncodeEndOfResults(header, inner.Q3))
}

func (j *Join) client(clientID string) *clientState {
	state, ok := j.clients[clientID]
	if !ok {
		state = &clientState{words: make(map[string]languages)}
		j.clients[clientID] = state
	}
	return state
}

func (j *Join) send(gatewayID string, body []byte) error {
	return j.mom.Publish(j.outputGatewayExchange, []string{gatewayID}, middleware.Message{Body: string(body)})
}
