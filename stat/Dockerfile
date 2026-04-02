FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /stat .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget

WORKDIR /app

COPY --from=builder /stat ./stat

ENV STAT_PORT=8002
# Default path inside the container; override with SQLITE_PATH. Persist /data via a volume (see docker-compose).
ENV SQLITE_PATH=/data/listenai.db

VOLUME ["/data"]

EXPOSE 8002

CMD ["./stat"]
