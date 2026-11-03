FROM golang:1.24-alpine3.23 AS builder
WORKDIR /build/
COPY go.mod go.sum ./
RUN go mod download
COPY common ./common
COPY accumulator ./accumulator
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/accumulator ./accumulator

FROM busybox:latest
COPY --from=builder /build/bin/accumulator /accumulator
ENTRYPOINT ["./accumulator"]
