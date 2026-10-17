package middleware

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	rabbitChannel *rabbitMQChannel
	exchangeName  string
	routingKeys   []string
	queueName     string // cola exclusiva de cada consumidor
}

func newMiddlewareExchange(rabbitChannel *rabbitMQChannel, exchangeName string, routingKeys []string) (*exchangeMiddleware, error) {
	err := rabbitChannel.channel.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		return nil, errors.Join(rabbitChannel.toMiddlewareError(err), rabbitChannel.close())
	}
	return &exchangeMiddleware{
		rabbitChannel: rabbitChannel,
		exchangeName:  exchangeName,
		routingKeys:   routingKeys,
	}, nil
}

/*
err := rabbitChannel.channel.ExchangeDeclare(
	exchangeName,        // name
	amqp.ExchangeDirect, // kind: entrega por routing key exacta
	false,               // durable: no sobrevive a un reinicio del broker
	false,               // autoDelete: no se borra al quedar sin colas
	false,               // internal: los clientes pueden publicarle
	false,               // noWait: espera la confirmación del broker
	nil,                 // args
)
*/

func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	// Si se vuelve a consumir tras un StopConsuming, se reusa la misma cola.
	if e.queueName == "" {
		queueName, err := e.subscribe()
		if err != nil {
			return err
		}
		e.queueName = queueName
	}
	return e.rabbitChannel.startConsuming(e.queueName, callbackFunc)
}

// subscribe declara una cola exclusiva con nombre generado por RabbitMQ y la
// bindea a cada routing key.
func (e *exchangeMiddleware) subscribe() (string, error) {
	channel := e.rabbitChannel.channel
	queue, err := channel.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return "", e.rabbitChannel.toMiddlewareError(err)
	}
	for _, key := range e.routingKeys {
		if err := channel.QueueBind(queue.Name, key, e.exchangeName, false, nil); err != nil {
			return "", e.rabbitChannel.toMiddlewareError(err)
		}
	}
	return queue.Name, nil
}

func (e *exchangeMiddleware) StopConsuming() error {
	return e.rabbitChannel.stopConsuming()
}

func (e *exchangeMiddleware) Send(msg Message) error {
	for _, key := range e.routingKeys {
		if err := e.rabbitChannel.publish(e.exchangeName, key, msg.Body); err != nil {
			return err
		}
	}
	return nil
}

func (e *exchangeMiddleware) Close() error {
	return e.rabbitChannel.close()
}
