package middleware

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const brokerURL = "amqp://guest:guest@%s:%d/"

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	rabbitChannel, err := connect(connectionSettings)

	if err != nil {
		return nil, err
	}
	return newMiddlewareQueue(rabbitChannel, queueName)
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	rabbitChannel, err := connect(connectionSettings)

	if err != nil {
		return nil, err
	}
	return newMiddlewareExchange(rabbitChannel, exchange, keys)
}

func CreateMultiRabbitMiddleware(connectionSettings ConnSettings) (MultiRabbitMiddleware, error) {
	rabbitChannel, err := connect(connectionSettings)

	if err != nil {
		return nil, err
	}
	return newMultiRabbitMiddleware(rabbitChannel.connection, rabbitChannel.channel), nil
}

func connect(connectionSettings ConnSettings) (*rabbitMQChannel, error) {
	url := fmt.Sprintf(brokerURL, connectionSettings.Hostname, connectionSettings.Port)
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}
	channel, err := connection.Channel()
	if err != nil {
		closeErr := connection.Close()
		return nil, errors.Join(fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err), closeErr)
	}
	return &rabbitMQChannel{connection: connection, channel: channel}, nil
}
