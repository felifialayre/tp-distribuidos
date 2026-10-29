package gateway

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"tp-distribuidos/common/middleware"
)

type GatewayConfig struct {
	GatewayID       string
	ServerPort      int
	OutputQueueName string
	MomHost         string
	MomPort         int
}

type Gateway struct {
	config      GatewayConfig
	listener    net.Listener
	outputQueue middleware.Middleware

	clients      map[string]*clientHandler
	nextClientID int
	mutex        sync.Mutex
	closing      bool
	wg           sync.WaitGroup
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
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, middlewareSettings)
	if err != nil {
		listener.Close()
		return nil, err
	}
	return &Gateway{config: config,
		listener:    listener,
		outputQueue: outputQueue,
		clients:     make(map[string]*clientHandler),
		closing:     false,
	}, nil
}

func (g *Gateway) Run() error {
	defer g.outputQueue.Close()
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

func (g *Gateway) Shutdown() {
	g.listener.Close()
	g.mutex.Lock()
	defer g.mutex.Unlock()

	g.closing = true
	for _, client := range g.clients {
		client.conn.Close()
	}
}

func (g *Gateway) addClient(conn net.Conn) (*clientHandler, bool) {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.closing {
		return nil, false
	}
	client := newClientHandler(g.nextID(), g.config.GatewayID, conn, g.outputQueue)
	g.clients[client.clientID] = client
	return client, true
}

func (g *Gateway) removeClient(clientID string) {
	g.mutex.Lock()
	delete(g.clients, clientID)
	g.mutex.Unlock()
}

func (g *Gateway) nextID() string {
	g.nextClientID++
	return fmt.Sprintf("%s-%d", g.config.GatewayID, g.nextClientID)
}

// corre la sesion del cliente y lo da de baja cuando termina
func (g *Gateway) handleClient(client *clientHandler) {
	defer g.wg.Done()
	defer g.removeClient(client.clientID)
	defer client.conn.Close()
	client.run()
}
