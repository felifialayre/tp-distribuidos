package middleware

import (
	"errors"
)

const prefetchCount = 1

type queueMiddleware struct {
	rabbitChannel *rabbitMQChannel
	queueName     string
}

/*
ch.QueueDeclare(
	queueName, // name
	false,      // durability (si sobrevive al reinicio de broker)
	false,     // delete when unused (si se borra cuando se va el ultimo consuimidor)
	false,     // exclusive (si la usan varias conexiones)
	false,     // no-wait (si espera confirmacion de la creacion de la queue)
	nil,       // args
}
*/

// Si falla la declaración de la cola o el QoS, se cierran el canal y la conexión.
func newMiddlewareQueue(rabbitChannel *rabbitMQChannel, queueName string) (*queueMiddleware, error) {
	channel := rabbitChannel.channel
	if _, err := channel.QueueDeclare(queueName, false, false, false, false, nil); err != nil {
		return nil, errors.Join(rabbitChannel.toMiddlewareError(err), rabbitChannel.close())
	}
	if err := channel.Qos(prefetchCount, 0, false); err != nil {
		return nil, errors.Join(rabbitChannel.toMiddlewareError(err), rabbitChannel.close())
	}
	return &queueMiddleware{rabbitChannel: rabbitChannel, queueName: queueName}, nil
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return q.rabbitChannel.startConsuming(q.queueName, callbackFunc)
}

func (q *queueMiddleware) StopConsuming() error {
	return q.rabbitChannel.stopConsuming()
}

func (q *queueMiddleware) Send(msg Message) error {
	// default exchange "" enruta directo a la cola con ese nombre
	return q.rabbitChannel.publish("", q.queueName, msg.Body)
}

func (q *queueMiddleware) Close() error {
	return q.rabbitChannel.close()
}
