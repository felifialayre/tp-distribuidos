package image_downloader

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// backoff entre reintentos: base * 2^(intento-1) (acotado a maxBackoff)
const (
	baseBackoff = 500 * time.Millisecond
	maxBackoff  = 8 * time.Second
)

// Fetcher baja las imagenes de los articulos con un GET directo.
// Como Wikimedia exige un User-Agent descriptivo se lo incluye (sino devuelve 403).
// Reintenta ante errores transitorios (red, timeout, 5xx, 429) con backoff.
type Fetcher struct {
	httpClient *http.Client
	userAgent  string
	maxRetries int
}

func NewFetcher(userAgent string, timeout time.Duration, maxRetries int) *Fetcher {
	return &Fetcher{
		httpClient: &http.Client{Timeout: timeout},
		userAgent:  userAgent,
		maxRetries: maxRetries,
	}
}

// FetchImage descarga la imagen en imageURL y devuelve sus bytes. Reintenta hasta
// maxRetries veces ante errores transitorios, si el error es definitivo corta en ese momento.
func (fetcher *Fetcher) FetchImage(imageURL string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= fetcher.maxRetries; attempt++ {
		if attempt > 0 {
			wait := backoff(attempt)
			slog.Warn("Reintentando descarga", "url", imageURL, "intento", attempt, "espera", wait, "err", lastErr)
			time.Sleep(wait)
		}
		body, retryable, err := fetcher.fetchOnce(imageURL)
		if err == nil {
			return body, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
	}
	return nil, fmt.Errorf("fetch: %q agoto los reintentos: %w", imageURL, lastErr)
}

// fetchOnce hace un intento. Devuelve si hubo error lo devuelve junto con si amerita reintento.
func (fetcher *Fetcher) fetchOnce(imageURL string) (body []byte, retryable bool, err error) {
	request, err := http.NewRequest(http.MethodGet, imageURL, nil)
	if err != nil {
		return nil, false, fmt.Errorf("fetch: build request %q: %w", imageURL, err)
	}
	request.Header.Set("User-Agent", fetcher.userAgent)

	response, err := fetcher.httpClient.Do(request)
	if err != nil {
		return nil, true, fmt.Errorf("fetch: get %q: %w", imageURL, err) // red/timeout: transitorio
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, isRetryableStatus(response.StatusCode), fmt.Errorf("fetch: get %q: status %d", imageURL, response.StatusCode)
	}

	body, err = io.ReadAll(response.Body)
	if err != nil {
		return nil, true, fmt.Errorf("fetch: read body %q: %w", imageURL, err) // corte durante la lectura: transitorio
	}
	return body, false, nil
}

// isRetryableStatus: 5xx (error del servidor) y 429 (rate limit) son transitorios,
// 4xx (404/410/403) significan que la imagen no esta o no se puede, no se reintenta.
func isRetryableStatus(status int) bool {
	return status >= 500 || status == http.StatusTooManyRequests
}

func backoff(attempt int) time.Duration {
	wait := baseBackoff << (attempt - 1) // base * 2^(attempt-1)
	if wait <= 0 || wait > maxBackoff {
		return maxBackoff
	}
	return wait
}
