package image_downloader

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// Fetcher baja las imagenes de los articulos con un GET directo a su URL
// como Wikimedia exige un User-Agent descriptivo se lo incluye (sino el devuelve 403).
type Fetcher struct {
	httpClient *http.Client
	userAgent  string
}

func NewFetcher(userAgent string, timeout time.Duration) *Fetcher {
	return &Fetcher{
		httpClient: &http.Client{Timeout: timeout},
		userAgent:  userAgent,
	}
}

// FetchImage descarga la imagen en imageURL y devuelve sus bytes.
func (fetcher *Fetcher) FetchImage(imageURL string) ([]byte, error) {
	request, err := http.NewRequest(http.MethodGet, imageURL, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch: build request %q: %w", imageURL, err)
	}
	request.Header.Set("User-Agent", fetcher.userAgent)

	response, err := fetcher.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch: get %q: %w", imageURL, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch: get %q: status %d", imageURL, response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("fetch: read body %q: %w", imageURL, err)
	}
	return body, nil
}
