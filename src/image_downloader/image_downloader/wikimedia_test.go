package image_downloader

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func testFetcher(maxRetries int) *Fetcher {
	return NewFetcher("test-agent", 5*time.Second, maxRetries)
}

// Un error transitorio (500) se reintenta y, si despues anda, devuelve los bytes.
func TestFetchRetriesThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 { // falla las primeras dos, anda la tercera
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte("imagen-ok"))
	}))
	defer server.Close()

	body, err := testFetcher(3).FetchImage(server.URL)
	if err != nil {
		t.Fatalf("esperaba exito tras reintentos, dio: %v", err)
	}
	if string(body) != "imagen-ok" {
		t.Fatalf("bytes incorrectos: %q", body)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("esperaba 3 intentos, hubo %d", got)
	}
}

// Un 404 es definitivo: NO se reintenta.
func TestFetchNoRetryOn404(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	if _, err := testFetcher(3).FetchImage(server.URL); err == nil {
		t.Fatal("esperaba error en 404")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("404 no debe reintentar, hubo %d intentos", got)
	}
}

// Un 500 permanente agota los reintentos (1 inicial + maxRetries).
func TestFetchGivesUpAfterRetries(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	if _, err := testFetcher(2).FetchImage(server.URL); err == nil {
		t.Fatal("esperaba error tras agotar reintentos")
	}
	if got := calls.Load(); got != 3 { // 1 inicial + 2 reintentos
		t.Fatalf("esperaba 3 intentos, hubo %d", got)
	}
}

func TestIsRetryableStatus(t *testing.T) {
	cases := map[int]bool{
		http.StatusInternalServerError: true,  // 500
		http.StatusBadGateway:          true,  // 502
		http.StatusTooManyRequests:     true,  // 429
		http.StatusNotFound:            false, // 404
		http.StatusGone:                false, // 410
		http.StatusForbidden:           false, // 403
	}
	for status, want := range cases {
		if got := isRetryableStatus(status); got != want {
			t.Errorf("isRetryableStatus(%d) = %v, esperaba %v", status, got, want)
		}
	}
}

func TestBackoffExponentialWithCap(t *testing.T) {
	if got := backoff(1); got != baseBackoff {
		t.Errorf("backoff(1) = %v, esperaba %v", got, baseBackoff)
	}
	if got := backoff(2); got != 2*baseBackoff {
		t.Errorf("backoff(2) = %v, esperaba %v", got, 2*baseBackoff)
	}
	if got := backoff(50); got != maxBackoff { // no se dispara ni overflowea
		t.Errorf("backoff(50) = %v, esperaba el tope %v", got, maxBackoff)
	}
}
