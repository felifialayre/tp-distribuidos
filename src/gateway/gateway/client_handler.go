package gateway

import (
	"fmt"
	"hash/crc32"
	"log/slog"
	"net"

	"tp-distribuidos/common/middleware"
	"tp-distribuidos/common/parquet_row"
	"tp-distribuidos/common/protocol/external"
	"tp-distribuidos/common/protocol/inner"
	"tp-distribuidos/common/socket"
)

type clientHandler struct {
	clientID   string
	gatewayID  string
	conn       net.Conn
	protocol   *external.ExternalProtocol
	mom        middleware.MultiRabbitMiddleware
	exchange   string   // exchange al que se publican las filas
	filterKeys []string // una clave de ruteo por replica del filter
	// por consulta, cuantos EndOfResults faltan (uno por replica)
	pending map[external.Query]int
}

func newClientHandler(clientID, gatewayID string, conn net.Conn, mom middleware.MultiRabbitMiddleware, exchange string, filterKeys []string) *clientHandler {
	return &clientHandler{
		clientID:   clientID,
		gatewayID:  gatewayID,
		conn:       conn,
		protocol:   external.NewExternalProtocol(socket.New(conn)),
		mom:        mom,
		exchange:   exchange,
		filterKeys: filterKeys,
		pending:    map[external.Query]int{external.Q1: len(filterKeys)},
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
	h.pending[query]--
	if h.pending[query] > 0 {
		return nil // faltan replicas por terminar esta consulta
	}
	delete(h.pending, query)
	return h.protocol.WriteEndOfResults(query)
}

func (h *clientHandler) complete() bool {
	return len(h.pending) == 0
}

func (h *clientHandler) publishRecord(row *parquet_row.ParquetRow) error {
	body := inner.EncodeRecord(h.header(), inner.Record{
		Name:         row.Name,
		URL:          row.URL,
		InLanguage:   row.InLanguage,
		DateCreated:  row.DateCreated,
		DateModified: row.DateModified,
		Sections:     row.Sections,
		Image:        row.Image,
		Abstract:     row.Abstract,
	})
	// la misma url va siempre a la misma replica
	key := h.filterKeys[crc32.ChecksumIEEE([]byte(row.URL))%uint32(len(h.filterKeys))]
	return h.mom.Publish(h.exchange, []string{key}, middleware.Message{Body: string(body)})
}

func (h *clientHandler) publishEndOfRecords() error {
	eof := inner.EncodeEndOfRecords(h.header())
	return h.mom.Publish(h.exchange, h.filterKeys, middleware.Message{Body: string(eof)}) // a todas las replicas
}

func (h *clientHandler) header() inner.Header {
	return inner.Header{ClientID: h.clientID, GatewayID: h.gatewayID}
}
