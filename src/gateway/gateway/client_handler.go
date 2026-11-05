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

	// los resultados no se mandan de a uno, se juntan por consulta hasta llegar a
	// resultBatchSize bytes o hasta que termina la consulta (llegan todos sus EndOfResults)
	resultBatchSize int
	batches         map[external.Query]*external.ResultBuffer
}

func newClientHandler(clientID, gatewayID string, conn net.Conn, mom middleware.MultiRabbitMiddleware, exchange string, filterKeys []string, joinAmount, aboveAverageAmount, idownloaderAmount, resultBatchSize int) *clientHandler {
	return &clientHandler{
		clientID:   clientID,
		gatewayID:  gatewayID,
		conn:       conn,
		protocol:   external.NewExternalProtocol(socket.New(conn)),
		mom:        mom,
		exchange:   exchange,
		filterKeys: filterKeys,
		pending:    map[external.Query]int{external.Q1: len(filterKeys), external.Q2: idownloaderAmount, external.Q3: joinAmount, external.Q4: 1, external.Q5: aboveAverageAmount},

		resultBatchSize: resultBatchSize,
		batches:         make(map[external.Query]*external.ResultBuffer),
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
	return h.addResult(external.Q1, external.Q1Row(url, dateModified))
}

func (h *clientHandler) sendQ2Result(result inner.Q2Result) error {
	return h.addResult(external.Q2, external.Q2Row(result.Name, result.URL, result.ImageBytes))
}

func (h *clientHandler) sendQ3Result(words []string) error {
	for _, word := range words {
		if err := h.addResult(external.Q3, external.Q3Row(word)); err != nil {
			return err
		}
	}
	return nil
}

func (h *clientHandler) sendQ4Result(top []inner.WordCount) error {
	for _, wc := range top {
		if err := h.addResult(external.Q4, external.Q4Row(wc.Word, wc.Count)); err != nil {
			return err
		}
	}
	return nil
}

func (h *clientHandler) sendQ5Result(articles []inner.Q5Article) error {
	for _, article := range articles {
		if err := h.addResult(external.Q5, external.Q5Row(article.URL, article.Count)); err != nil {
			return err
		}
	}
	return nil
}

// addResult agrega una fila al batch de su consulta. El batch sale cuando llega a
// resultBatchSize bytes, si la fila no entra en lo que queda, sale antes de agregarla,
// asi un batch no pasa el tamano salvo que una sola fila ya lo pase.
func (h *clientHandler) addResult(query external.Query, fields []string) error {
	batch, ok := h.batches[query]
	if !ok {
		batch = external.NewResultBuffer(query)
		h.batches[query] = batch
	}

	if batch.Len() > 0 && batch.Size()+external.RowSize(fields) > h.resultBatchSize {
		if err := h.flush(query); err != nil {
			return err
		}
	}
	if err := batch.Add(fields); err != nil {
		return err
	}

	return nil
}

// flush manda lo que haya juntado de la consulta, si hay algo
func (h *clientHandler) flush(query external.Query) error {
	batch, ok := h.batches[query]
	if !ok || batch.Len() == 0 {
		return nil
	}
	if err := h.protocol.WriteResultBatch(batch); err != nil {
		return err
	}
	batch.Reset()
	return nil
}

func (h *clientHandler) sendEndOfResults(query external.Query) error {
	h.pending[query]--
	if h.pending[query] > 0 {
		return nil // faltan replicas por terminar esta consulta
	}
	delete(h.pending, query)

	// lo que quedo juntando sale antes del EndOfResults, el cliente recibe todas las filas y despues el fin
	if err := h.flush(query); err != nil {
		return err
	}

	delete(h.batches, query)
	return h.protocol.WriteEndOfResults(query)
}

func (h *clientHandler) complete() bool {
	return len(h.pending) == 0
}

func (h *clientHandler) publishRecord(row *parquet_row.ParquetRow) error {
	body := inner.EncodeRecord(h.header(), inner.Record{
		Name:         row.Name,
		URL:          row.URL,
		InLanguage:   row.InLanguage.Identifier,
		DateCreated:  row.DateCreated,
		DateModified: row.DateModified,
		SectionNames: row.SectionNames(),
		ImageURL:     row.Image.ContentURL,
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
