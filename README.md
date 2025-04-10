# go-events-publisher

Publishes events to a kafka broker.

## Build

`go build` in the root directory will create the `go-events-publisher` executable.

## Run tests

`go test -p 1 -tags ci ./...` will run all unit tests.

For running the integration test you will have to set the correct environment variables 
(or use a `.env.local` file). Example `.env.local` file in the root folder:

```dotenv
QUBIC_EVENTS_PUBLISHER_CLIENT_EVENT_API_URL="localhost:8003"
```

## Configuration options

You can use command line properties or environment variables. Environment variables need to be prefixed with `QUBIC_EVENTS_PUBLISHER_`.

Example:

```bash
./go-events-publisher \
--client-event-api-url=localhost:8003 \
--broker-bootstrap-servers=localhost:9092 \
--broker-metrics-port=9999 \
--broker-metrics-namespace=qubic-events \
--broker-produce-topic=qubic-events \
--sync-internal-store-folder=store \
--sync-start-epoch=153
```

`
--client-event-api-url=
`
Host and port of the event service grpc endpoint.

`
--broker-bootstrap-servers=
`
Kafka bootstrap server urls.

`
--broker-metrics-port=
`
Port for exposing prometheus metrics. Defaults to 9999. Access default with `curl localhost:9999/metrics` for example.

`
--broker-metrics-namespace=
`
Namespace (prefix) for prometheus metrics.

`
--broker-produce-topic=
`
Target topic for the produced event kafka messages.

`
--sync-internal-store-folder=
`
Folder for the embedded database. Stores metadata, like last processed tick per epoch.

`
--sync-start-epoch=
`
Epoch number to start syncing from.
