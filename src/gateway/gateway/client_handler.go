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
	mom       middleware.MultiRabbitMiddleware
	output    string // cola a la que se publican las filas (proximamente un exchange)
	pending   map[external.Query]bool
}

func newClientHandler(clientID, gatewayID string, conn net.Conn, mom middleware.MultiRabbitMiddleware, output string) *clientHandler {
	return &clientHandler{
		clientID:  clientID,
		gatewayID: gatewayID,
		conn:      conn,
		protocol:  external.NewExternalProtocol(socket.New(conn)),
		mom:       mom,
		output:    output,
		pending:   map[external.Query]bool{external.Q1: true},
	}
}

func (h *clientHandler) run() bool {
	for {
		msgType, err := h.protocol.ReadMsgType()
		if err != nil {
			slog.Warn("Client disconnected", "client", h.clientID, "err", err)
			return false
		}

		switch msgType {
		case external.Batch:
			err = h.handleBatch()
		case external.EndOfRecords:
			err = h.handleEndOfRecords()
			if err == nil {
				return true // el cliente termino de mandar
			}
		default:
			err = fmt.Errorf("unexpected message type %d", msgType)
		}
		if err != nil {
			slog.Error("Handling client", "client", h.clientID, "err", err)
			return false
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
	return nil
}

func (h *clientHandler) handleEndOfRecords() error {
	slog.Info("End of records", "client", h.clientID)
	return h.publishEndOfRecords()
}

func (h *clientHandler) sendQ1Result(url, dateModified string) error {
	return h.protocol.WriteQ1Result(url, dateModified)
}

func (h *clientHandler) sendEndOfResults(query external.Query) error {
	if err := h.protocol.WriteEndOfResults(query); err != nil {
		return err
	}
	delete(h.pending, query)
	return nil
}

func (h *clientHandler) complete() bool {
	return len(h.pending) == 0
}

func (h *clientHandler) publishRecord(row *parquet_row.ParquetRow) error {
	body := inner.EncodeRecord(h.header(), inner.Record{
		URL:          row.URL,
		InLanguage:   row.InLanguage,
		DateCreated:  row.DateCreated,
		DateModified: row.DateModified,
		Sections:     row.Sections,
		Image:        row.Image,
		Abstract:     row.Abstract,
	})
	return h.mom.SendToQueue(h.output, middleware.Message{Body: string(body)})
}

func (h *clientHandler) publishEndOfRecords() error {
	eof := inner.EncodeEndOfRecords(h.header())
	return h.mom.SendToQueue(h.output, middleware.Message{Body: string(eof)})
}

func (h *clientHandler) header() inner.Header {
	return inner.Header{ClientID: h.clientID, GatewayID: h.gatewayID}
}
