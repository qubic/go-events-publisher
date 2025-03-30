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
	reader := NewEventReader(eventClient, store)
	epoch, err := reader.sync(115, 1)
	assert.NoError(t, err)
	assert.Equal(t, 120, int(epoch))

	epoch, err = reader.sync(120, 1)
	assert.NoError(t, err)
	assert.Equal(t, 120, int(epoch))

	epoch, err = reader.sync(120, 1)
	assert.NoError(t, err)
	assert.Equal(t, 123, int(epoch))

}

func TestMain(m *testing.M) {
	// slog.SetLogLoggerLevel(slog.LevelDebug)

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
