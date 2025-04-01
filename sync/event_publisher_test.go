package sync

import (
	"context"
	eventspb "github.com/qubic/go-events/proto"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

type FakeKafkaClient struct {
	produceErr error
}

func (fkc *FakeKafkaClient) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	promise(r, fkc.produceErr)
}

func TestEventPublisher_ProcessTickEvents(t *testing.T) {

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
