FROM golang:1.23 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/go-events-publisher
COPY . /src/go-events-publisher

RUN go mod tidy
WORKDIR /src/go-events-publisher
RUN go build

FROM ubuntu:latest
LABEL authors="mio@qubic.org"

# copy executable from build stage
COPY --from=builder /src/go-events-publisher/go-events-publisher /app/go-events-publisher

RUN chmod +x /app/go-events-publisher

WORKDIR /app

ENTRYPOINT ["./go-events-publisher"]