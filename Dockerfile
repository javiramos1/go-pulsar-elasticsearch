FROM golang:1.17-alpine3.15 AS builder

RUN apk update && \
    apk add --no-cache --update alpine-sdk git

ARG CGO_ENABLED=0

WORKDIR /app

COPY go.* ./
RUN go mod download

COPY . .
RUN GOOS=linux GOARCH=amd64 go build -mod=readonly -o dist/go-pulsar-elasticsearch -v -ldflags "-w -s" .

FROM gcr.io/distroless/static

COPY .env /etc/
ADD tmp/schema /etc/schema

COPY --from=builder /app/dist/go-pulsar-elasticsearch /usr/local/bin/go-pulsar-elasticsearch

CMD ["/usr/local/bin/go-pulsar-elasticsearch"]