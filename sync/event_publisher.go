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
	Epoch           uint32
	Tick            uint32
	EventId         uint64
	EventDigest     uint64
	TransactionHash string
	EventType       uint32
	EventSize       uint32
	EventData       string
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

func (ep *EventPublisher) ProcessTickEvents(ctx context.Context, tickEvents *eventspb.TickEvents) (int, error) {
	var sentEvents int
	tick := tickEvents.Tick
	wg := sync.WaitGroup{}

	// we create the records first
	// advantage: we send after successfully converting. disadvantage: records in memory
	for _, transactionEvents := range tickEvents.TxEvents {
		transactionHash := transactionEvents.TxId
		log.Printf("Processing events of transaction [%s]: [%d].", transactionHash, len(transactionEvents.Events))
		for _, e := range transactionEvents.Events {

			record, err := createEventRecord(e, tick, transactionHash)
			if err != nil {
				return 0, errors.Wrapf(err, "creating record for event [%d]", e.Header.EventId)
			}

			wg.Add(1)
			var sendError error
			ep.kcl.Produce(nil, record, func(_ *kgo.Record, err error) {
				defer wg.Done()
				if err != nil {
					sendError = errors.Wrapf(err, "Error sending record for event [%d].", e.Header.EventId)
				} else {
					sentEvents++
				}
			})
			// Be aware: if the producer has no information if the message was delivered (like network down) it will hang
			// here indefinitely until the network is back up. No error will be produced in this case.
			wg.Wait()
			if sendError != nil {
				return sentEvents, errors.Wrap(err, "publishing events.")
			}

		}

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

	//hash, err := hashEvent(&event)
	//if err != nil {
	//    return nil, errors.Wrap(err, "failed to hash event")
	//}
	//return base64.StdEncoding.EncodeToString(hash[:]), nil

	payload, err := json.Marshal(event)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal event")
	}
	key := make([]byte, 4)
	binary.LittleEndian.PutUint32(key, tick)
	record := &kgo.Record{Key: key, Topic: "qubic-events", Value: payload}
	return record, nil
}

//func hashEvent(event *Event) ([]byte, error) {
//    var buff bytes.Buffer
//    err := binary.Write(&buff, binary.LittleEndian, event.Epoch)
//    if err != nil {
//        return nil, errors.Wrap(err, "writing epoch to buffer")
//    }
//    err = binary.Write(&buff, binary.LittleEndian, event.Tick)
//    if err != nil {
//        return nil, errors.Wrap(err, "writing tick to buffer")
//    }
//    err = binary.Write(&buff, binary.LittleEndian, event.EventId)
//    if err != nil {
//        return nil, errors.Wrap(err, "writing event id to buffer")
//    }
//    err = binary.Write(&buff, binary.LittleEndian, event.EventDigest)
//    if err != nil {
//        return nil, errors.Wrap(err, "writing event digest to buffer")
//    }
//    _, err = buff.Write([]byte(event.TransactionHash))
//    if err != nil {
//        return nil, errors.Wrap(err, "writing transaction hash to buffer")
//    }
//    err = binary.Write(&buff, binary.LittleEndian, event.EventType)
//    if err != nil {
//        return nil, errors.Wrap(err, "writing event type to buffer")
//    }
//    err = binary.Write(&buff, binary.LittleEndian, event.EventSize)
//    if err != nil {
//        return nil, errors.Wrap(err, "writing event size to buffer")
//    }
//    _, err = buff.Write([]byte(event.EventData))
//    if err != nil {
//        return nil, errors.Wrap(err, "writing event data to buffer")
//    }
//    hash, err := common.K12Hash(buff.Bytes())
//    return hash[:], err
//
//}
