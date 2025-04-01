package sync

import (
	"context"
	"errors"
	eventspb "github.com/qubic/go-events/proto"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

type FakeKafkaClient struct {
	produceErr   error
	sentMessages int
}

func (fkc *FakeKafkaClient) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	if fkc.produceErr == nil {
		fkc.sentMessages++
	}
	promise(r, fkc.produceErr)
}

func TestEventPublisher_ProcessTickEvents(t *testing.T) {

	kafkaClient := &FakeKafkaClient{}

	pub := EventPublisher{
		kcl: kafkaClient,
	}

	transactionEvents1 := eventspb.TransactionEvents{
		TxId: "tx-id-1",
		Events: []*eventspb.Event{
			{Header: &eventspb.Event_Header{}},
			{Header: &eventspb.Event_Header{}},
		},
	}

	transactionEvents2 := eventspb.TransactionEvents{
		TxId: "tx-id-2",
		Events: []*eventspb.Event{
			{Header: &eventspb.Event_Header{}},
			{Header: &eventspb.Event_Header{}},
			{Header: &eventspb.Event_Header{}},
		},
	}

	tickEvents := eventspb.TickEvents{
		Tick: 12345,
		TxEvents: []*eventspb.TransactionEvents{
			&transactionEvents1,
			&transactionEvents2,
		},
	}

	count, err := pub.ProcessTickEvents(context.Background(), &tickEvents)
	assert.NoError(t, err)
	assert.Equal(t, 5, count)
	assert.Equal(t, 5, kafkaClient.sentMessages)

}

func TestEventPublisher_ProcessTickEvents_GivenError_ThenReturn(t *testing.T) {

	kafkaClient := &FakeKafkaClient{
		produceErr: errors.New("test error"),
	}

	pub := EventPublisher{
		kcl: kafkaClient,
	}

	transactionEvents := eventspb.TransactionEvents{
		TxId: "tx-id",
		Events: []*eventspb.Event{
			{Header: &eventspb.Event_Header{}},
			{Header: &eventspb.Event_Header{}},
		},
	}

	tickEvents := eventspb.TickEvents{
		Tick: 12345,
		TxEvents: []*eventspb.TransactionEvents{
			&transactionEvents,
		},
	}

	count, err := pub.ProcessTickEvents(context.Background(), &tickEvents)
	assert.Error(t, err)
	assert.Equal(t, 0, count)
	assert.Equal(t, 0, kafkaClient.sentMessages)

}

func TestEventPublisher_ProcessTickEvents_GivenNoEvent_ThenReturnZeroProcessed(t *testing.T) {

	kafkaClient := &FakeKafkaClient{}

	pub := EventPublisher{
		kcl: kafkaClient,
	}

	tickEvents := eventspb.TickEvents{
		Tick:     12345,
		TxEvents: nil,
	}

	count, err := pub.ProcessTickEvents(context.Background(), &tickEvents)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}
