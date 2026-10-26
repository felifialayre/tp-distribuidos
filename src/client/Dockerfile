FROM golang:1.24-alpine3.23 AS builder
WORKDIR /build/
COPY go.mod go.sum ./
COPY common ./common
COPY client ./client
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/client ./client

FROM busybox:latest
COPY --from=builder /build/bin/client /client
ENTRYPOINT ["./client"]
