package middleware

import "errors"

var (
	ErrMessageMiddlewareMessage      = errors.New("message middleware: message error")
	ErrMessageMiddlewareDisconnected = errors.New("message middleware: disconnected")
	ErrMessageMiddlewareClose        = errors.New("message middleware: close error")
	ErrMessageMiddlewareChannel      = errors.New("message middleware: channel error")
	ErrMessageMiddlewareConsuming    = errors.New("message middleware: middleware already consuming")
	ErrMessageMiddlewareEmptyKeys    = errors.New("message middleware: empty keys")
	ErrMessageMiddlewarePublish      = errors.New("message middleware: publish error")
)

type Message struct {
	Body string
}

type ConnSettings struct {
	Hostname string
	Port     int
}

type Middleware interface {

	//	Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
	//	cada mensaje de datos o de control con el cuerpo del mensaje.
	//	callbackFunc tiene como parámetro:
	//	msg - El struct tal y como lo recibe el método Send.
	//	ack - Una función que hace ACK del mensaje recibido.
	//	nack - Una función que hace NACK del mensaje recibido.
	//	Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	//	Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
	StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error

	//	Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	//	no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	//	Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	StopConsuming() error

	//	Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
	//	Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
	//	Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
	Send(msg Message) error

	//	Se desconecta de la cola o exchange al que estaba conectado.
	//	Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
	Close() error
}

type MultiRabbitMiddleware interface {

	// AddInputQueue Crea una fuente de consumo de una Queue
	AddInputQueue(name string, callback Callback) error
	// AddInputExchange Crea una fuente de consumo de un Exchange
	AddInputExchange(name string, kind ExchangeKind, keys []string, callback Callback) error

	// AddOutputQueue Declara una Queue para producción
	AddOutputQueue(name string) error
	// AddOutputExchange Declara un Exchange para producción
	AddOutputExchange(name string, kind ExchangeKind) error

	// SendToQueue Envía a la Queue por su nombre
	SendToQueue(name string, msg Message) error
	// Publish Publica al Exchange por su nombre y routing key
	Publish(exchange string, keys []string, msg Message) error

	StartConsuming() error
	StopConsuming() error
	Close() error
}
