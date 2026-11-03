FROM golang:1.24-alpine3.23 AS builder
WORKDIR /build/
COPY go.mod go.sum ./
RUN go mod download
COPY common ./common
COPY join_average ./join_average
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/join_average ./join_average

FROM busybox:latest
COPY --from=builder /build/bin/join_average /join_average
ENTRYPOINT ["./join_average"]
