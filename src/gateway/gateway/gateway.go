package gateway

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/google/uuid"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/external"
	"tp-distribuidos/common/protocol/inner"
)

type GatewayConfig struct {
	GatewayID       string
	ServerPort      int
	FilterExchange  string
	FilterPrefix    string
	FilterAmount    int
	JoinAmount      int
	ResultsExchange string
	MomHost         string
	MomPort         int
}

type Gateway struct {
	config   GatewayConfig
	listener net.Listener
	mom      middleware.MultiRabbitMiddleware
	clients  map[string]*clientHandler
	mutex    sync.Mutex
	closing  bool
	wg       sync.WaitGroup
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.ServerPort))
	if err != nil {
		return nil, err
	}
	middlewareSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}
	mom, err := middleware.CreateMultiRabbitMiddleware(middlewareSettings)
	if err != nil {
		listener.Close()
		return nil, err
	}

	g := &Gateway{config: config,
		listener: listener,
		mom:      mom,
		clients:  make(map[string]*clientHandler),
		closing:  false,
	}

	if err := mom.AddOutputExchange(config.FilterExchange, middleware.Direct); err != nil {
		mom.Close()
		listener.Close()
		return nil, err
	}
	if err := mom.AddInputExchange(config.ResultsExchange, middleware.Direct, []string{config.GatewayID}, g.callback); err != nil {
		mom.Close()
		listener.Close()
		return nil, err
	}
	return g, nil
}

func (g *Gateway) Run() error {
	defer g.mom.Close()
	go g.consumeResults()

	for {
		conn, err := g.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break // cerro el listener
			}
			g.Shutdown()
			g.wg.Wait()
			return err
		}

		client, ok := g.addClient(conn)
		if !ok {
			_ = conn.Close()
			continue
		}
		g.wg.Add(1)
		go g.handleClient(client)
	}
	g.wg.Wait()
	return nil
}

func (g *Gateway) consumeResults() {
	if err := g.mom.StartConsuming(); err != nil {
		slog.Error("Consuming results", "err", err)
		g.Shutdown()
	}
}

func (g *Gateway) Shutdown() {
	g.listener.Close()

	g.mutex.Lock()
	g.closing = true
	for _, client := range g.clients {
		client.conn.Close()
	}
	g.mutex.Unlock()

	_ = g.mom.StopConsuming()
}

func (g *Gateway) addClient(conn net.Conn) (*clientHandler, bool) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.closing {
		return nil, false
	}
	keys := make([]string, g.config.FilterAmount)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s_%d", g.config.FilterPrefix, i)
	}
	client := newClientHandler(uuid.NewString(), g.config.GatewayID, conn, g.mom, g.config.FilterExchange, keys, g.config.JoinAmount)
	g.clients[client.clientID] = client
	return client, true
}

func (g *Gateway) getClient(clientID string) *clientHandler {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	return g.clients[clientID]
}

func (g *Gateway) removeClient(clientID string) {
	g.mutex.Lock()
	delete(g.clients, clientID)
	g.mutex.Unlock()
}

// corre la sesion del cliente
func (g *Gateway) handleClient(client *clientHandler) {
	defer g.wg.Done()
	if !client.run() {
		g.closeClient(client)
	}
}

func (g *Gateway) closeClient(client *clientHandler) {
	client.conn.Close()
	g.removeClient(client.clientID)
}

func (g *Gateway) callback(msg middleware.Message, ack func(), nack func()) {
	if err := g.dispatchResult([]byte(msg.Body)); err != nil {
		slog.Error("Handling result", "err", err)
		nack()
		return
	}
	ack()
}

func (g *Gateway) dispatchResult(data []byte) error {
	op, err := inner.ReadOpcode(data)
	if err != nil {
		return err
	}

	switch op {
	case inner.OpQ1Result:
		header, result, err := inner.DecodeQ1Result(data)
		if err != nil {
			return err
		}
		g.sendToClient(header.ClientID, func(client *clientHandler) error {
			return client.sendQ1Result(result.URL, result.DateModified)
		})
		return nil
	case inner.OpQ3Result:
		header, result, err := inner.DecodeQ3Result(data)
		if err != nil {
			return err
		}
		g.sendToClient(header.ClientID, func(client *clientHandler) error {
			return client.sendQ3Result(result.Words)
		})
		return nil
	case inner.OpEndOfResults:
		header, query, err := inner.DecodeEndOfResults(data)
		if err != nil {
			return err
		}
		externalQuery, err := toExternalQuery(query)
		if err != nil {
			return err
		}
		g.sendToClient(header.ClientID, func(client *clientHandler) error {
			return client.sendEndOfResults(externalQuery)
		})
		return nil
	default:
		return inner.ErrUnexpectedOpcode
	}
}

func (g *Gateway) sendToClient(clientID string, send func(*clientHandler) error) {
	client := g.getClient(clientID)
	if client == nil {
		return
	}
	if err := send(client); err != nil {
		slog.Warn("Sending result to client", "client", clientID, "err", err)
		g.closeClient(client)
		return
	}
	if client.complete() {
		g.closeClient(client)
	}
}

func toExternalQuery(q inner.Query) (external.Query, error) {
	switch q {
	case inner.Q1:
		return external.Q1, nil
	case inner.Q3:
		return external.Q3, nil
	default:
		return 0, fmt.Errorf("unknown query %d", q)
	}
}
