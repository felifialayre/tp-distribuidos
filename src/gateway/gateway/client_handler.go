package gateway

import (
	"fmt"
	"log/slog"
	"net"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/parquet_row"
	"tp-distribuidos/common/protocol/external"
	"tp-distribuidos/common/protocol/inner"
	"tp-distribuidos/common/socket"
)

type clientHandler struct {
	clientID  string
	gatewayID string
	conn      net.Conn
	protocol  *external.ExternalProtocol
	output    middleware.Middleware
	records   int
}

func newClientHandler(clientID, gatewayID string, conn net.Conn, output middleware.Middleware) *clientHandler {
	return &clientHandler{
		clientID:  clientID,
		gatewayID: gatewayID,
		conn:      conn,
		protocol:  external.NewExternalProtocol(socket.New(conn)),
		output:    output,
	}
}

func (h *clientHandler) run() {
	for {
		msgType, err := h.protocol.ReadMsgType()
		if err != nil {
			slog.Warn("Client disconnected", "client", h.clientID, "records", h.records, "err", err)
			return
		}

		switch msgType {
		case external.Batch:
			err = h.handleBatch()
		case external.EndOfRecords:
			err = h.handleEndOfRecords()
			if err == nil {
				return // el cliente termino de mandar
			}
		default:
			err = fmt.Errorf("unexpected message type %d", msgType)
		}
		if err != nil {
			slog.Error("Handling client", "client", h.clientID, "err", err)
			return
		}
	}
}

func (h *clientHandler) handleBatch() error {
	rows, err := h.protocol.ReadBatch()
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := h.publishRecord(row); err != nil {
			return err
		}
	}
	h.records += len(rows)
	return h.protocol.WriteAck()
}

func (h *clientHandler) handleEndOfRecords() error {
	slog.Info("End of records", "client", h.clientID, "records", h.records)
	if err := h.publishEndOfRecords(); err != nil {
		return err
	}
	return h.protocol.WriteAck()
}

// nota: el Message o el Header podria darmelo el protocol
func (h *clientHandler) publishRecord(row *parquet_row.ParquetRow) error {
	body, err := inner.EncodeRecord(h.header(), row)
	if err != nil {
		return err
	}
	return h.output.Send(middleware.Message{Body: string(body)})
}

func (h *clientHandler) publishEndOfRecords() error {
	eof := inner.EncodeEndOfRecords(h.header())
	return h.output.Send(middleware.Message{Body: string(eof)})
}

func (h *clientHandler) header() inner.Header {
	return inner.Header{ClientID: h.clientID, GatewayID: h.gatewayID}
}
