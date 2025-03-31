package sync

import (
	"context"
	"flag"
	"github.com/qubic/go-events-publisher/client"
	eventspb "github.com/qubic/go-events/proto"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

var store *PebbleStore

type FakeEventClient struct {
	status *client.EventStatus
	events map[uint32]*eventspb.TickEvents
}

func (client *FakeEventClient) GetEvents(_ context.Context, tickNumber uint32) (*eventspb.TickEvents, error) {
	return client.events[tickNumber], nil
}

func (client *FakeEventClient) GetStatus(_ context.Context) (*client.EventStatus, error) {
	return client.status, nil
}

type FakeEventProcessor struct {
	processedCount int
}

func (p *FakeEventProcessor) ProcessTickEvents(_ context.Context, tickEvents *eventspb.TickEvents) error {
	if tickEvents == nil {
		p.processedCount++
	} else {
		p.processedCount += len(tickEvents.TxEvents)
	}
	return nil
}

func TestEventReader_sync(t *testing.T) {

	intervals := map[uint32][]*client.ProcessedTickInterval{
		120: {{From: 1230, To: 1233}, {From: 1234, To: 1234}},
		123: {{From: 12340, To: 12345}},
	}

	eventClient := &FakeEventClient{
		status: &client.EventStatus{
			Epoch:     123,
			Tick:      12345,
			Intervals: intervals,
		},
		events: map[uint32]*eventspb.TickEvents{},
	}

	eventProcessor := FakeEventProcessor{}
	reader := NewEventReader(eventClient, &eventProcessor, store)
	epoch, err := reader.sync(115, 1)
	assert.NoError(t, err)
	assert.Equal(t, 120, int(epoch))

	assert.Equal(t, 4, eventProcessor.processedCount) // 4 ticks

	epoch, err = reader.sync(120, 1)
	assert.NoError(t, err)
	assert.Equal(t, 120, int(epoch))

	assert.Equal(t, 5, eventProcessor.processedCount) // 1 tick

	epoch, err = reader.sync(120, 1)
	assert.NoError(t, err)
	assert.Equal(t, 123, int(epoch))

	assert.Equal(t, 11, eventProcessor.processedCount) // 6 ticks

	// clean up
	err = reader.dataStore.deleteLastProcessedTicks(120, 124)
	assert.NoError(t, err)
}

func TestEventReader_calculateTickRanges(t *testing.T) {
	intervals := map[uint32][]*client.ProcessedTickInterval{
		119: {{From: 1, To: 100}},
		120: {{From: 1230, To: 1233}, {From: 1234, To: 1234}},
		123: {{From: 12340, To: 12345}},
	}

	eventClient := &FakeEventClient{
		status: &client.EventStatus{
			Epoch:     123,
			Tick:      12345,
			Intervals: intervals,
		},
		events: map[uint32]*eventspb.TickEvents{},
	}

	reader := NewEventReader(eventClient, &FakeEventProcessor{}, store)
	start, end, epoch, err := reader.calculateTickRange(context.Background(), 120)
	assert.NoError(t, err)
	assert.Equal(t, 120, int(epoch))
	assert.Equal(t, 1230, int(start))
	assert.Equal(t, 1233, int(end))

	err = reader.dataStore.SetLastProcessedTick(120, 1233)
	assert.NoError(t, err)

	start, end, epoch, err = reader.calculateTickRange(context.Background(), 120)
	assert.NoError(t, err)
	assert.Equal(t, 120, int(epoch))
	assert.Equal(t, 1234, int(start))
	assert.Equal(t, 1234, int(end))

	err = reader.dataStore.SetLastProcessedTick(120, 1234)
	assert.NoError(t, err)

	start, end, epoch, err = reader.calculateTickRange(context.Background(), 120)
	assert.NoError(t, err)
	assert.Equal(t, 123, int(epoch))
	assert.Equal(t, 12340, int(start))
	assert.Equal(t, 12345, int(end))

	err = reader.dataStore.SetLastProcessedTick(123, 12345)
	assert.NoError(t, err)

	start, end, epoch, err = reader.calculateTickRange(context.Background(), 120)
	assert.NoError(t, err)
	assert.Equal(t, 0, int(epoch))
	assert.Equal(t, 0, int(start))
	assert.Equal(t, 0, int(end))

	// clean up
	err = reader.dataStore.deleteLastProcessedTicks(120, 124)
	assert.NoError(t, err)

}

func TestMain(m *testing.M) {

	// we could use a fake db but in memory works, too
	// needs cleaning after every test
	tempDir, err := os.MkdirTemp("", "processor_store_test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	store, err = NewPebbleStore(tempDir)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// Parse args and run
	flag.Parse()
	exitCode := m.Run()
	// Exit
	os.Exit(exitCode)
}
