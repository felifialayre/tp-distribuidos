package middleware

import (
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type consumer struct {
	channel     *amqp.Channel
	queueName   string // cola a consumir (en exchange, la cola exclusiva)
	consumerTag string // para Cancel en StopConsuming
	callback    Callback
}

type multiMiddleware struct {
	connection  *amqp.Connection
	consumers   []*consumer
	sendChannel *amqp.Channel
	consuming   bool
	stopped     bool
	mutex       sync.Mutex
}

type Callback func(msg Message, ack func(), nack func())

type ExchangeKind int

const (
	Direct ExchangeKind = iota
	Fanout
	Topic
)

func (k ExchangeKind) amqpKind() string {
	switch k {
	case Fanout:
		return amqp.ExchangeFanout
	case Topic:
		return amqp.ExchangeTopic
	default:
		return amqp.ExchangeDirect
	}
}

// toMiddlewareError traduce un error de amqp al error de middleware que corresponde.
func (middleware *multiMiddleware) toMiddlewareError(err error) error {
	if err == nil {
		return nil
	}
	if middleware.connection.IsClosed() {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}
	return fmt.Errorf("%w: %v", ErrMessageMiddlewareMessage, err)
}

func newMultiRabbitMiddleware(connection *amqp.Connection, channel *amqp.Channel) *multiMiddleware {
	return &multiMiddleware{connection: connection, sendChannel: channel, consuming: false, stopped: false}
}

func (middleware *multiMiddleware) AddInputQueue(queueName string, callback Callback) error {
	if middleware.consuming {
		return ErrMessageMiddlewareConsuming
	}

	channel, err := middleware.connection.Channel()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareChannel, err)
	}
	if _, err := channel.QueueDeclare(queueName, false, false, false, false, nil); err != nil {
		return errors.Join(middleware.toMiddlewareError(err), channel.Close())
	}
	if err := channel.Qos(prefetchCount, 0, false); err != nil {
		return errors.Join(middleware.toMiddlewareError(err), channel.Close())
	}

	middleware.consumers = append(middleware.consumers, &consumer{
		channel:     channel,
		queueName:   queueName,
		consumerTag: queueName,
		callback:    callback,
	})
	return nil
}

func (middleware *multiMiddleware) AddInputExchange(exchangeName string, kind ExchangeKind, keys []string, callback Callback) error {
	if middleware.consuming {
		return ErrMessageMiddlewareConsuming
	}

	if len(keys) == 0 {
		return ErrMessageMiddlewareEmptyKeys
	}

	channel, err := middleware.connection.Channel()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareChannel, err)
	}

	err = channel.ExchangeDeclare(exchangeName, kind.amqpKind(), false, false, false, false, nil)
	if err != nil {
		return errors.Join(middleware.toMiddlewareError(err), channel.Close())
	}

	queue, err := channel.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return errors.Join(middleware.toMiddlewareError(err), channel.Close())
	}
	if err := channel.Qos(prefetchCount, 0, false); err != nil {
		return errors.Join(middleware.toMiddlewareError(err), channel.Close())
	}
	for _, key := range keys {
		if err := channel.QueueBind(queue.Name, key, exchangeName, false, nil); err != nil {
			return errors.Join(middleware.toMiddlewareError(err), channel.Close())
		}
	}

	middleware.consumers = append(middleware.consumers, &consumer{
		channel:     channel,
		queueName:   queue.Name,
		consumerTag: exchangeName,
		callback:    callback,
	})
	return nil
}

func (middleware *multiMiddleware) AddOutputQueue(name string) error {
	if middleware.consuming {
		return ErrMessageMiddlewareConsuming
	}
	_, err := middleware.sendChannel.QueueDeclare(name, false, false, false, false, nil)
	return middleware.toMiddlewareError(err)
}

func (middleware *multiMiddleware) AddOutputExchange(name string, kind ExchangeKind) error {
	if middleware.consuming {
		return ErrMessageMiddlewareConsuming
	}
	err := middleware.sendChannel.ExchangeDeclare(name, kind.amqpKind(), false, false, false, false, nil)
	return middleware.toMiddlewareError(err)
}

func (middleware *multiMiddleware) SendToQueue(name string, msg Message) error {
	if middleware.connection.IsClosed() || middleware.sendChannel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	err := middleware.sendChannel.Publish("", name, false, false, amqp.Publishing{
		DeliveryMode: amqp.Transient,
		ContentType:  "text/plain",
		Body:         []byte(msg.Body),
	})
	return middleware.toMiddlewareError(err)
}

func (middleware *multiMiddleware) Publish(exchange string, keys []string, msg Message) error {
	if middleware.connection.IsClosed() || middleware.sendChannel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	var errs error
	for _, key := range keys {
		err := middleware.sendChannel.Publish(exchange, key, false, false, amqp.Publishing{
			DeliveryMode: amqp.Transient,
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
		})
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("%w: exchange=%q key=%q: %v", ErrMessageMiddlewarePublish, exchange, key, err))
		}
	}
	return errs
}

type pendingDelivery struct {
	delivery amqp.Delivery
	callback Callback
}

type activeConsumer struct {
	consumer   *consumer
	deliveries <-chan amqp.Delivery
}

func (middleware *multiMiddleware) StartConsuming() error {
	middleware.mutex.Lock()

	if middleware.stopped {
		middleware.stopped = false
		middleware.mutex.Unlock()
		return nil
	}
	if middleware.consuming {
		middleware.mutex.Unlock()
		return ErrMessageMiddlewareConsuming
	}

	consumers := middleware.consumers // ya nadie puede appendear

	activeConsumers, err := middleware.startConsumers(consumers)
	if err != nil {
		middleware.mutex.Unlock()
		return err
	}
	middleware.consuming = true
	middleware.mutex.Unlock()

	inbox := make(chan pendingDelivery)
	var wg sync.WaitGroup
	wg.Add(len(activeConsumers)) // voy a esperar a N goroutines
	for _, anActiveConsumer := range activeConsumers {
		go func(anActiveConsumer activeConsumer) {
			defer wg.Done()
			for delivery := range anActiveConsumer.deliveries { // se cierra con Cancel o caída
				inbox <- pendingDelivery{delivery: delivery, callback: anActiveConsumer.consumer.callback}
			}
		}(anActiveConsumer)
	}
	go func() { wg.Wait(); close(inbox) }() // cierra merged cuando salieron TODOS

	// DISPATCHER ==> este goroutine (bloquea, un callback a la vez)
	for pending := range inbox {
		pending.callback(
			Message{Body: string(pending.delivery.Body)},
			func() { pending.delivery.Ack(false) },
			func() { pending.delivery.Nack(false, false) },
		)
	}

	// el loop terminó, a propósito o caída?
	middleware.mutex.Lock()
	stopped := middleware.stopped
	middleware.consuming = false
	middleware.stopped = false
	middleware.mutex.Unlock()
	if stopped {
		return nil
	}
	return ErrMessageMiddlewareDisconnected
}

func (middleware *multiMiddleware) startConsumers(consumers []*consumer) ([]activeConsumer, error) {
	var activeConsumers []activeConsumer
	for _, c := range consumers {
		deliveries, err := c.channel.Consume(c.queueName, c.consumerTag, false, false, false, false, nil)
		if err != nil {
			for _, a := range activeConsumers {
				a.consumer.channel.Cancel(a.consumer.consumerTag, false)
			}
			return nil, middleware.toMiddlewareError(err)
		}
		activeConsumers = append(activeConsumers, activeConsumer{consumer: c, deliveries: deliveries})
	}
	return activeConsumers, nil
}

func (middleware *multiMiddleware) StopConsuming() error {
	middleware.mutex.Lock()
	middleware.stopped = true
	if !middleware.consuming {
		middleware.mutex.Unlock()
		return nil
	}
	consumers := middleware.consumers
	middleware.mutex.Unlock()

	var errs error
	for _, c := range consumers {
		if err := c.channel.Cancel(c.consumerTag, false); err != nil {
			errs = errors.Join(errs, middleware.toMiddlewareError(err))
		}
	}
	return errs
}

func (middleware *multiMiddleware) Close() error {
	_ = middleware.StopConsuming()

	var errs error
	for _, c := range middleware.consumers {
		if err := c.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			errs = errors.Join(errs, err)
		}
	}
	if err := middleware.sendChannel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		errs = errors.Join(errs, err)
	}
	if !middleware.connection.IsClosed() {
		if err := middleware.connection.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	if errs != nil {
		return errors.Join(ErrMessageMiddlewareClose, errs)
	}
	return nil
}
