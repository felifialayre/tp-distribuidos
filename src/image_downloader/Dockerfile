FROM golang:1.24-alpine3.23 AS builder
WORKDIR /build/
# ca-certificates: el bundle de CAs para validar TLS en las descargas a través de Wikimedia (HTTPS)
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY common ./common
COPY image_downloader ./image_downloader
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/image_downloader ./image_downloader

FROM busybox:latest
# busybox no trae el bundle de CAs, sin esto el GET a la API falla.
# por ahora lo dejo así pero más adelante capaz lo tengo q cambiar
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /build/bin/image_downloader /image_downloader
ENTRYPOINT ["./image_downloader"]
