package image_downloader

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

// Downloader baja las imagenes de los articulos con un GET directo a su URL
// como Wikimedia exige un User-Agent descriptivo se lo incluye (sino el devuelve 403).
type Downloader struct {
	httpClient *http.Client
	userAgent  string
}

func NewDownloader(userAgent string, timeout time.Duration) *Downloader {
	return &Downloader{
		httpClient: &http.Client{Timeout: timeout},
		userAgent:  userAgent,
	}
}

// FetchImage descarga la imagen en imageURL y devuelve sus bytes.
func (downloader *Downloader) FetchImage(imageURL string) ([]byte, error) {
	request, err := http.NewRequest(http.MethodGet, imageURL, nil)
	if err != nil {
		return nil, fmt.Errorf("download: build request %q: %w", imageURL, err)
	}
	request.Header.Set("User-Agent", downloader.userAgent)

	response, err := downloader.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("download: get %q: %w", imageURL, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download: get %q: status %d", imageURL, response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("download: read body %q: %w", imageURL, err)
	}
	return body, nil
}
