FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o f1-collector ./cmd/collector
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o f1-sink ./cmd/sink

FROM alpine:3.19
WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /app/f1-collector .
COPY --from=builder /app/f1-sink .

COPY --from=builder /app/web ./web

COPY --from=builder /app/config.json .

EXPOSE 20777/udp
EXPOSE 8080/tcp

CMD ["./f1-collector"]
