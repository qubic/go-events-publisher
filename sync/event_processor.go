package sync

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	eventspb "github.com/qubic/go-events/proto"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
)

type Event struct {
	Epoch           uint32
	Tick            uint32
	EventId         uint64
	EventDigest     uint64
	TransactionHash string
	EventType       uint32
	EventSize       uint32
	EventData       string
}

type EventProcessor interface {
	ProcessTickEvents(ctx context.Context, tickEvents *eventspb.TickEvents) error
}

type KafkaClient interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

type EventPublisher struct {
	kafkaClient KafkaClient
}

func NewEventPublisher(client KafkaClient) *EventPublisher {
	return &EventPublisher{
		kafkaClient: client,
	}
}

func (ep *EventPublisher) ProcessTickEvents(ctx context.Context, tickEvents *eventspb.TickEvents) error {
	var events []*Event
	var eventCount int
	for _, transactionEvents := range tickEvents.TxEvents {
		eventCount += len(transactionEvents.Events)
		transactionHash := transactionEvents.TxId
		log.Printf("Processing events for transaction [%s].", transactionHash)

		for _, e := range transactionEvents.Events {
			event := Event{
				Epoch:           e.Header.Epoch,
				Tick:            e.Header.Tick,
				EventId:         e.Header.EventId,
				EventDigest:     e.Header.EventDigest,
				TransactionHash: transactionHash,
				EventType:       e.EventType,
				EventSize:       e.EventSize,
				EventData:       e.EventData,
			}
			log.Printf("Processing event [%d] with type %d", event.EventId, event.EventType)
			events = append(events, &event)
		}

	}

	if len(events) > 0 {

		payload, err := json.Marshal(events)
		if err != nil {
			return errors.Wrap(err, "failed to marshal events")
		}

		key := make([]byte, 4)
		binary.LittleEndian.PutUint32(key, tickEvents.Tick)
		record := &kgo.Record{Key: key, Topic: "qubic-events", Value: payload}

		// TODO change this to async later
		if err = ep.kafkaClient.ProduceSync(ctx, record).FirstErr(); err != nil {
			fmt.Printf("record had a produce error while synchronously producing: %v\n", err)
		}
		log.Printf("Processed: tick [%d] with [%d] transactions and [%d] events.", tickEvents.Tick, len(tickEvents.TxEvents), eventCount)
	} else {
		log.Printf("No events in tick [%d].", tickEvents.Tick)
	}

	return nil
}
