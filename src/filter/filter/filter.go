package filter

import (
	"fmt"
	"tp-distribuidos/common/middleware"
)

type Filter struct {
	inputQueue middleware.Middleware
}

func NewFilter(queueName string, host string, port int) (*Filter, error) {
	middlewareSettings := middleware.ConnSettings{
		Hostname: host,
		Port:     port,
	}
	inputQueue, err := middleware.CreateQueueMiddleware(queueName, middlewareSettings)
	if err != nil {
		return nil, err
	}
	return &Filter{inputQueue}, nil
}

func (f *Filter) Run() error {
	defer f.inputQueue.Close()
	return f.inputQueue.StartConsuming(func(msg middleware.Message, ack func(), nack func()) {
		fmt.Println("recibí:", msg.Body)
		ack()
	})
}
