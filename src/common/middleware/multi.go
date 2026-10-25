package middleware

import (
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type consumer struct {
	channel  *amqp.Channel
	queue    string // cola a consumir (en exchange, la cola exclusiva)
	tag      string // para Cancel en StopConsuming
	callback Callback
}

type multiMiddleware struct {
	connection  *amqp.Connection
	recvSources []*consumer
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

type event struct {
	inputMessage amqp.Delivery
	callback     Callback
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

func (middleware *multiMiddleware) ConsumeQueue(queueName string, callback Callback) error {
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

	middleware.recvSources = append(middleware.recvSources, &consumer{
		channel:  channel,
		queue:    queueName,
		tag:      queueName,
		callback: callback,
	})
	return nil
}

func (middleware *multiMiddleware) ConsumeExchange(exchangeName string, kind ExchangeKind, keys []string, callback Callback) error {
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

	middleware.recvSources = append(middleware.recvSources, &consumer{
		channel:  channel,
		queue:    queue.Name,
		tag:      exchangeName,
		callback: callback,
	})
	return nil
}

func (middleware *multiMiddleware) DeclareQueue(name string) error {
	if middleware.consuming {
		return ErrMessageMiddlewareConsuming
	}
	_, err := middleware.sendChannel.QueueDeclare(name, false, false, false, false, nil)
	return middleware.toMiddlewareError(err)
}

func (middleware *multiMiddleware) DeclareExchange(name string, kind ExchangeKind) error {
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

func (middleware *multiMiddleware) StartConsuming() error {
	middleware.mutex.Lock()
	if middleware.consuming {
		middleware.mutex.Unlock()
		return ErrMessageMiddlewareConsuming
	}
	middleware.consuming = true
	srcs := middleware.recvSources // ya nadie puede appendear
	middleware.mutex.Unlock()

	// abrir el Consume de cada fuente
	type stream struct {
		consumerData *consumer
		deliveries   <-chan amqp.Delivery
	}
	var streams []stream
	for _, c := range srcs {
		deliveries, err := c.channel.Consume(c.queue, c.tag, false /*autoAck*/, false, false, false, nil)
		if err != nil {
			// cancelar los que YA abrí y abortar
			for _, s := range streams {
				s.consumerData.channel.Cancel(s.consumerData.tag, false)
			}
			middleware.mutex.Lock()
			middleware.consuming = false
			middleware.mutex.Unlock()
			return middleware.toMiddlewareError(err)
		}
		streams = append(streams, stream{c, deliveries})
	}

	// Un reader por fuente -> merged
	merged := make(chan event)
	var wg sync.WaitGroup
	wg.Add(len(streams)) // voy a esperar a N goroutines
	for _, s := range streams {
		go func(s stream) {
			defer wg.Done()
			for d := range s.deliveries { // se cierra con Cancel o caída
				merged <- event{inputMessage: d, callback: s.consumerData.callback}
			}
		}(s)
	}
	go func() { wg.Wait(); close(merged) }() // cierra merged cuando salieron TODOS

	// DISPATCHER ==> este goroutine (bloquea, un callback a la vez)
	for ev := range merged {
		inputMsg := ev.inputMessage
		ev.callback(
			Message{Body: string(inputMsg.Body)},
			func() { inputMsg.Ack(false) },
			func() { inputMsg.Nack(false, false) },
		)
	}

	// el loop terminó, a propósito o caída?
	middleware.mutex.Lock()
	stopped := middleware.stopped
	middleware.consuming = false
	middleware.mutex.Unlock()
	if stopped {
		return nil
	}
	return ErrMessageMiddlewareDisconnected
}

func (middleware *multiMiddleware) StopConsuming() error {
	middleware.mutex.Lock()
	if !middleware.consuming {
		middleware.mutex.Unlock()
		return nil
	}
	middleware.stopped = true
	srcs := middleware.recvSources
	middleware.mutex.Unlock()

	var errs error
	for _, c := range srcs {
		if err := c.channel.Cancel(c.tag, false); err != nil {
			errs = errors.Join(errs, middleware.toMiddlewareError(err))
		}
	}
	return errs
}

func (middleware *multiMiddleware) Close() error {
	_ = middleware.StopConsuming()

	var errs error
	for _, c := range middleware.recvSources {
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
