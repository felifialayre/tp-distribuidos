FROM golang:1.24-alpine3.23 AS builder
# cd build, si no existe la crea
WORKDIR /build/
# las copia desde el context a build
COPY go.mod go.sum ./
RUN go mod download
# copia src/common/ a /build/common/
COPY common ./common
COPY gateway ./gateway
# genera ejecutable
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/gateway ./gateway

#otra imagen para correr las cosas, ya que con solo el ejecutable
# esta img pesa mucho menos
# img liviana de linux
FROM busybox:latest
# me llevo el ejecutable de la primera etapa aca
COPY --from=builder /build/bin/gateway /gateway
# defino que se ejcute cuando el contenedor arranque
ENTRYPOINT ["./gateway"]
