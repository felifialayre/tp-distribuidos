package middleware

import (
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// rabbitMQChannel agrupa la conexión, el canal y el estado de consumo que
// comparten la cola y el exchange.
type rabbitMQChannel struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	// consumerTag es "" cuando no se está consumiendo. Lo protege mutex porque
	// StopConsuming suele llamarse desde otra goroutine (por ejemplo, un SIGTERM).
	mutex       sync.Mutex
	consumerTag string
}

// toMiddlewareError traduce un error de amqp al error de middleware que corresponde.
func (r *rabbitMQChannel) toMiddlewareError(err error) error {
	if err == nil {
		return nil
	}
	if r.connection.IsClosed() {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}
	return fmt.Errorf("%w: %v", ErrMessageMiddlewareMessage, err)
}

func (r *rabbitMQChannel) publish(exchange string, routingKey string, body string) error {
	if r.connection.IsClosed() || r.channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	err := r.channel.Publish(exchange, routingKey, false, false, amqp.Publishing{
		DeliveryMode: amqp.Transient,
		ContentType:  "text/plain",
		Body:         []byte(body),
	})
	return r.toMiddlewareError(err)
}

//En pika BlockingConnection no es thread-safe. Lo que haciamos channel.start_consuming() es un loop
//escondido adentro de la librería, se bloquea y llama al callback por cada mensaje.
//Y para cortarlo, se llamaba a stop_consuming() desde el callback o desde el handler de sigterm
//( que se corre mid ejecucion).

// En amqp091 es seguro llamar a diferentes metodos del canal desde distintas goroutines(threads).
//channel.Consume(...) no se bloquea ni llama a nada. Sino que te e devuelve un channel de Go
// (<-chan amqp.Delivery), que es como una cola en memoria por donde la librería
//te va dejando los mensajes. El loop lo escribís vos:
//for ... range sobre un channel significa: “dame el próximo mensaje, y si no hay, esperá”.
//El loop termina solo cuando el channel se cierra, y la librería lo cierra en dos casos:
//Cuando cancelás el consumidor (channel.Cancel(...)), o sea en stopConsuming.
//Cuando se cae el canal o la conexión.
//tambien puede cortar si hubo un panic en la funcion de callback

// consumerTag cumple dos funciones:
//  1. Es el nombre con el que se registra el consumidor en RabbitMQ (Consume).
//     Hace falta para darlo de baja después (Cancel), porque un canal puede
//     tener varios consumidores. Uso el nombre de la cola, que es único.
//  2. ConsumerTag es la bandera de "estoy consumiendo": "" = no, cualquier otra cosa = sí.
//    	Cuando el loop de startConsuming termina, no sabe por qué: la librería
// 		cierra deliveries tanto si se canceló el consumidor (stopConsuming) como si
// 		se cayó el canal o la conexión. Para distinguirlo, se usa consumerTag como nota:
//   	- stopConsuming la borra (consumerTag = "") ANTES de llamar a Cancel.
//   	- Al salir del loop, startConsuming la mira:
//       "" → alguien pasó por stopConsuming: fue a propósito, devuelve nil.
//       "cola" → nadie la borró: se cayó la conexión, devuelve Disconnected.
//     	En los dos casos la deja en "" para poder volver a consumir.
// Lo protege el mutex porque lo tocan las dos goroutines. (start y stop)

func (r *rabbitMQChannel) startConsuming(queueName string, callbackFunc func(msg Message, ack func(), nack func())) error {
	r.mutex.Lock()
	if r.consumerTag != "" {
		r.mutex.Unlock()
		return fmt.Errorf("%w: already consuming", ErrMessageMiddlewareMessage)
	}
	if r.connection.IsClosed() {
		r.mutex.Unlock()
		return ErrMessageMiddlewareDisconnected
	}
	deliveries, err := r.channel.Consume(queueName, queueName, false, false, false, false, nil)
	if err != nil {
		r.mutex.Unlock()
		return r.toMiddlewareError(err)
	}
	r.consumerTag = queueName
	r.mutex.Unlock()

	// Si el callback hace panic, se detiene el consumidor antes de descartar
	defer func() {
		if p := recover(); p != nil {
			_ = r.stopConsuming()
			panic(p)
		}
	}()

	for delivery := range deliveries {
		callbackFunc(
			Message{Body: string(delivery.Body)},
			func() { _ = delivery.Ack(false) },
			func() { _ = delivery.Nack(false, false) },
			// requeue false porque si el msj esta corrompido el cliente nunca lo procesa y se manda infinitamente
		)
	}

	// deliveries se cierra por stopConsuming (que ya limpió el tag) o porque
	// se cayó el canal o la conexión.
	r.mutex.Lock()
	wasConsuming := r.consumerTag != ""
	r.consumerTag = ""
	r.mutex.Unlock()
	if wasConsuming {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

/*
channel.Consume(
	queueName, // nombre cola
	queueName, //el tag
	false, //autoAck (El ack lo manda el callback a mano)
	false, //exclusive (Otros consumidores pueden usar la misma cola)
	false, //noLocal (rabbitMQ no lo soporta, siempre va en false)
	false, //noWait (espera la confirmación del broker)
	nil //args
	)
*/

func (r *rabbitMQChannel) stopConsuming() error {
	r.mutex.Lock()
	consumerTag := r.consumerTag
	r.consumerTag = ""
	r.mutex.Unlock()
	if consumerTag == "" {
		return nil
	}
	if err := r.channel.Cancel(consumerTag, false); err != nil {
		return fmt.Errorf("%w: %v", ErrMessageMiddlewareDisconnected, err)
	}
	return nil
}

// close cierra el canal y la conexión por separado: aunque falle el cierre
// del canal, la conexión se cierra igual. Cerrar algo que ya estaba cerrado
// (por ejemplo, tras una desconexión) no se considera un error.
func (r *rabbitMQChannel) close() error {
	_ = r.stopConsuming()
	channelErr := r.channel.Close()
	var connectionErr error
	if !r.connection.IsClosed() {
		connectionErr = r.connection.Close()
	}
	if errors.Is(connectionErr, amqp.ErrClosed) {
		connectionErr = nil
	}
	if channelErr != nil || connectionErr != nil {
		return errors.Join(ErrMessageMiddlewareClose, channelErr, connectionErr)
	}
	return nil
}
