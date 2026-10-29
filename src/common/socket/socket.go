package socket

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"
)

const network = "tcp"

var ErrClosed = errors.New("socket: connection closed")

type Socket struct {
	conn         net.Conn
	reader       *bufio.Reader
	closed       atomic.Bool
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// New crea un Socket a partir de una conexion ya establecida
func New(conn net.Conn) *Socket {
	return &Socket{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}
}

// Dial se conecta a un servidor
func Dial(address string, timeout time.Duration) (*Socket, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, fmt.Errorf("socket: dial %s: %w", address, err)
	}
	return New(conn), nil
}

// Receive lee exactamente len(buf) bytes o devuelve error
func (s *Socket) Receive(buf []byte) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if s.ReadTimeout > 0 {
		_ = s.conn.SetReadDeadline(time.Now().Add(s.ReadTimeout))
	}
	_, err := io.ReadFull(s.reader, buf)
	return err
}

// Send escribe todos los bytes o devuelve error.
func (s *Socket) Send(data []byte) error {
	n, err := s.write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// Close cierra la conexion
func (s *Socket) Close() error {
	//Swap devuelve el valor anterior y carga el nuevo
	if s.closed.Swap(true) {
		return nil
	}
	return s.conn.Close()
}

// write escribe todos los bytes en la conexion (io.Writer)
func (s *Socket) write(data []byte) (int, error) {
	if s.closed.Load() {
		return 0, ErrClosed
	}
	if s.WriteTimeout > 0 {
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.WriteTimeout))
	}
	return s.conn.Write(data)
}
