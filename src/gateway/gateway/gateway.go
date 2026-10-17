package gateway

import (
	"fmt"
	"tp-distribuidos/common/middleware"
)

type Gateway struct {
	outputQueue middleware.Middleware
}

type GatewayConfig struct {
	OutputQueueName string
	MomHost         string
	MomPort         int
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	middlewareSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}
	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, middlewareSettings)
	if err != nil {
		return nil, err
	}
	return &Gateway{outputQueue}, nil
}

func (g *Gateway) Run() error {
	defer g.outputQueue.Close()
	fmt.Println("gateway mandando mensaje")
	return g.outputQueue.Send(middleware.Message{Body: "hola bro"})
}
