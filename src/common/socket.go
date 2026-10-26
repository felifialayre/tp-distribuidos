package socket

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"time"
)

const network = "tcp"

var ErrClosed = errors.New("socket: connection closed")

type Socket struct {
	conn   net.Conn
	reader *bufio.Reader
	closed bool
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

// Write escribe todos los bytes en la conexion (io.Writer)
func (s *Socket) Write(data []byte) (int, error) {
	if s.closed {
		return 0, ErrClosed
	}
	if s.WriteTimeout > 0 {
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.WriteTimeout))
	}
	return s.conn.Write(data)
}

// Read lee hasta len(buf) bytes y devuelve cuantos leyo (io.Reader)
func (s *Socket) Read(buf []byte) (int, error) {
	if s.closed {
		return 0, ErrClosed
	}
	if s.ReadTimeout > 0 {
		_ = s.conn.SetReadDeadline(time.Now().Add(s.ReadTimeout))
	}
	return s.reader.Read(buf)
}

// Close cierra la conexion
func (s *Socket) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.conn.Close()
}
