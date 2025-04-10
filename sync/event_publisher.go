package sync

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/pkg/errors"
	eventspb "github.com/qubic/go-events/proto"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"sync"
)

type Event struct {
	Epoch           uint32 `json:"epoch"`
	Tick            uint32 `json:"tick"`
	EventId         uint64 `json:"eventId"`
	EventDigest     uint64 `json:"eventDigest"`
	TransactionHash string `json:"transactionHash"`
	EventType       uint32 `json:"eventType"`
	EventSize       uint32 `json:"eventSize"`
	EventData       string `json:"eventData"`
}

type Publisher interface {
	ProcessTickEvents(ctx context.Context, tickEvents *eventspb.TickEvents) (int, error)
}

type KafkaClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
}

type EventPublisher struct {
	kcl KafkaClient
}

func NewEventPublisher(client KafkaClient) *EventPublisher {
	return &EventPublisher{
		kcl: client,
	}
}

func (ep *EventPublisher) ProcessTickEvents(_ context.Context, tickEvents *eventspb.TickEvents) (int, error) {
	var sentEvents int
	tick := tickEvents.Tick
	wg := sync.WaitGroup{}

	var errs []error
	for _, transactionEvents := range tickEvents.TxEvents {
		transactionHash := transactionEvents.TxId
		// log.Printf("Processing events of transaction [%s]: [%d].", transactionHash, len(transactionEvents.Events))

		for _, e := range transactionEvents.Events {

			eventId := e.Header.EventId
			record, err := createEventRecord(e, tick, transactionHash)
			if err != nil {
				createError := errors.Wrapf(err, "creating message for tick [%d] transaction [%s] event [%d]", tick, transactionHash, eventId)
				log.Printf("Error %v", createError)
				errs = append(errs, createError)
				break
			}

			wg.Add(1)
			ep.kcl.Produce(nil, record, func(_ *kgo.Record, err error) {
				defer wg.Done()
				if err != nil {
					sendError := errors.Wrapf(err, "sending message for tick [%d] transaction [%s] event [%d]", tick, transactionHash, eventId)
					log.Printf("Error %v", sendError)
					errs = append(errs, sendError)
				} else {
					sentEvents++
				}
			})
			// Be aware: if the producer has no information if the message was delivered (like network down) it will hang
			// here indefinitely until the network is back up. No error will be produced in this case.
		}

		// in case we encounter an error don't proceed with next transaction
		if len(errs) > 0 {
			log.Printf("Aborting sending events for tick [%d] because of error(s).", tick)
			break
		}

	}

	// wait at end of tick (performance vs. error handling)
	wg.Wait()
	if len(errs) > 0 {
		return sentEvents, errors.Errorf("[%d] error(s) sending messages for tick [%d]", len(errs), tick)
	}

	return sentEvents, nil
}

func createEventRecord(sourceEvent *eventspb.Event, tick uint32, transactionHash string) (*kgo.Record, error) {
	event := Event{
		Epoch:           sourceEvent.Header.Epoch,
		Tick:            tick,
		EventId:         sourceEvent.Header.EventId,
		EventDigest:     sourceEvent.Header.EventDigest,
		TransactionHash: transactionHash,
		EventType:       sourceEvent.EventType,
		EventSize:       sourceEvent.EventSize,
		EventData:       sourceEvent.EventData,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal event")
	}

	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, tick)

	record := &kgo.Record{Key: key, Value: payload}
	return record, nil
}
