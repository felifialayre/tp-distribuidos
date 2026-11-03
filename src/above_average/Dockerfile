FROM golang:1.24-alpine3.23 AS builder
WORKDIR /build/
COPY go.mod go.sum ./
RUN go mod download
COPY common ./common
COPY above_average ./above_average
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/above_average ./above_average

FROM busybox:latest
COPY --from=builder /build/bin/above_average /above_average
ENTRYPOINT ["./above_average"]
