package filter

import (
	"fmt"
	"log/slog"
	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/protocol/inner"
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
		header, row, err := inner.DecodeRecord([]byte(msg.Body))
		if err != nil {
			slog.Error("Decoding record", "err", err)
			nack()
			return
		}
		fmt.Println("recibí:", header.ClientID, row.URL)
		ack()
	})
}
