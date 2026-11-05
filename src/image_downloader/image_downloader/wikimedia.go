package image_downloader

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// backoff entre reintentos: base * 2^(intento-1) (acotado a maxBackoff)
const (
	baseBackoff = 500 * time.Millisecond
	maxBackoff  = 8 * time.Second
)

// Fetcher baja las imagenes de los articulos con un GET directo.
// Como Wikimedia exige un User-Agent descriptivo se lo incluye (sino devuelve 403).
// Reintenta ante errores transitorios (red, timeout, 429) con backoff,
// o esperando lo que pida el header Retry-After si viene en la respuesta.
type Fetcher struct {
	httpClient *http.Client
	userAgent  string
	maxRetries int
	limiter    *rateLimiter // nil si no hay limite de velocidad
}

// maxMbps es el limite de velocidad de descarga entre todos los workers (0 = sin limite)
func NewFetcher(userAgent string, timeout time.Duration, maxRetries int, maxMbps int) *Fetcher {
	fetcher := &Fetcher{
		httpClient: &http.Client{Timeout: timeout},
		userAgent:  userAgent,
		maxRetries: maxRetries,
	}
	if maxMbps > 0 {
		fetcher.limiter = &rateLimiter{bytesPerSecond: float64(maxMbps) * 1e6 / 8}
	}
	return fetcher
}

// FetchImage descarga la imagen en imageURL y devuelve sus bytes. Reintenta hasta
// maxRetries veces ante errores transitorios, si el error es definitivo corta en ese momento.
func (fetcher *Fetcher) FetchImage(imageURL string) ([]byte, error) {
	var lastErr error
	var retryAfter time.Duration
	for attempt := 0; attempt <= fetcher.maxRetries; attempt++ {
		if attempt > 0 {
			wait := backoff(attempt)
			if retryAfter > 0 {
				wait = retryAfter
			}
			slog.Warn("Reintentando descarga", "url", imageURL, "intento", attempt, "espera", wait, "err", lastErr)
			time.Sleep(wait)
		}
		body, retryable, after, err := fetcher.fetchOnce(imageURL)
		if err == nil {
			return body, nil
		}
		lastErr = err
		retryAfter = after
		if !retryable {
			return nil, err
		}
	}
	return nil, fmt.Errorf("fetch: %q agoto los reintentos: %w", imageURL, lastErr)
}

// fetchOnce hace un intento. Devuelve si hubo error lo devuelve junto con si amerita reintento
// y cuanto esperar segun el Retry-After (0 si no vino).
func (fetcher *Fetcher) fetchOnce(imageURL string) (body []byte, retryable bool, retryAfter time.Duration, err error) {
	request, err := http.NewRequest(http.MethodGet, imageURL, nil)
	if err != nil {
		return nil, false, 0, fmt.Errorf("fetch: build request %q: %w", imageURL, err)
	}
	request.Header.Set("User-Agent", fetcher.userAgent)

	response, err := fetcher.httpClient.Do(request)
	if err != nil {
		return nil, true, 0, fmt.Errorf("fetch: get %q: %w", imageURL, err) // red/timeout: transitorio
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, isRetryableStatus(response.StatusCode), parseRetryAfter(response.Header), fmt.Errorf("fetch: get %q: status %d", imageURL, response.StatusCode)
	}

	var reader io.Reader = response.Body
	if fetcher.limiter != nil {
		reader = limitedReader{reader: reader, limiter: fetcher.limiter}
	}
	body, err = io.ReadAll(reader)
	if err != nil {
		return nil, true, 0, fmt.Errorf("fetch: read body %q: %w", imageURL, err) // corte durante la lectura: transitorio
	}
	return body, false, 0, nil
}

// isRetryableStatus: 5xx (error del servidor) y 429 (rate limit) son transitorios,
// 4xx (404/410/403) significan que la imagen no esta o no se puede, no se reintenta.
func isRetryableStatus(status int) bool {
	return status >= 500 || status == http.StatusTooManyRequests
}

// parseRetryAfter lee el header Retry-After, que puede venir en segundos o como fecha HTTP.
func parseRetryAfter(header http.Header) time.Duration {
	value := header.Get("Retry-After")
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil {
		return time.Until(date)
	}
	return 0
}

// rateLimiter limita los bytes por segundo que se bajan entre todos los workers,
// por cada pedazo leido corre el momento permitido y duerme si se adelanto.
type rateLimiter struct {
	mutex          sync.Mutex
	bytesPerSecond float64
	next           time.Time // momento a partir del cual lo ya leido respeta el limite
}

func (limiter *rateLimiter) wait(n int) {
	limiter.mutex.Lock()
	now := time.Now()
	if limiter.next.Before(now) {
		limiter.next = now
	}
	limiter.next = limiter.next.Add(time.Duration(float64(n) / limiter.bytesPerSecond * float64(time.Second)))
	wait := limiter.next.Sub(now)
	limiter.mutex.Unlock()
	time.Sleep(wait)
}

// limitedReader lee del body pasando por el rateLimiter
type limitedReader struct {
	reader  io.Reader
	limiter *rateLimiter
}

func (r limitedReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.limiter.wait(n)
	return n, err
}

func backoff(attempt int) time.Duration {
	wait := baseBackoff << (attempt - 1) // base * 2^(attempt-1)
	if wait <= 0 || wait > maxBackoff {
		return maxBackoff
	}
	return wait
}
