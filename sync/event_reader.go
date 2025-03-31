package sync

import (
	"context"
	"github.com/pkg/errors"
	"github.com/qubic/go-events-publisher/client"
	eventspb "github.com/qubic/go-events/proto"
	"log"
	"time"
)

type EventClient interface {
	GetEvents(ctx context.Context, tickNumber uint32) (*eventspb.TickEvents, error)
	GetStatus(ctx context.Context) (*client.EventStatus, error)
}
type EventReader struct {
	eventClient EventClient
	dataStore   DataStore
}

func NewEventReader(client EventClient, store DataStore) *EventReader {
	es := EventReader{
		eventClient: client,
		dataStore:   store,
	}
	return &es
}

func (r *EventReader) SyncInLoop(startEpoch uint32) {
	var count uint64
	epoch := startEpoch
	loopTick := time.Tick(time.Second * 1)
	for range loopTick {
		latestProcessedEpoch, err := r.sync(epoch, count)
		if err != nil {
			log.Printf("sync failed for epoch %d: %v", epoch, err)
		}
		epoch = latestProcessedEpoch
		count++
		time.Sleep(time.Second)
	}
}

func (r *EventReader) sync(startEpoch uint32, count uint64) (uint32, error) {
	log.Printf("Sync run: %d", count)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	start, end, epoch, err := r.calculateTickRange(ctx, startEpoch)
	if err != nil {
		return startEpoch, errors.Wrap(err, "Error calculating tick range")
	}

	if start > end || start == 0 || end == 0 || epoch == 0 {
		log.Printf("No ticks to process. Start: %d, end %d, epoch: %d", start, end, epoch)
	} else { // if start == end then process one tick
		log.Printf("Processing ticks from %d to %d for epoch %d", start, end, epoch)
		err = r.dataStore.SetLastProcessedTick(epoch, end) // TODO set after processing of each tick
		log.Printf("Set last processed tick: %d/%d", epoch, end)
		if err != nil {
			return startEpoch, errors.Wrap(err, "Error setting last processed tick")
		}
	}

	return epoch, nil
}

func (r *EventReader) calculateTickRange(ctx context.Context, startEpoch uint32) (uint32, uint32, uint32, error) {

	// get status from event service
	eventStatus, err := r.eventClient.GetStatus(ctx)
	if err != nil {
		return 0, 0, startEpoch, errors.Wrap(err, "calling event service")
	}

	// find first tick that is not stored yet
	searchEpoch := min(startEpoch, eventStatus.Epoch)

	// find first tick interval to process
	for searchEpoch <= eventStatus.Epoch {
		tickIntervals := eventStatus.Intervals[searchEpoch]
		if tickIntervals == nil {
			// nothing to sync
			searchEpoch++

		} else {

			lastProcessedTick, err := r.dataStore.GetLastProcessedTick(searchEpoch)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return 0, 0, searchEpoch, errors.Wrap(err, "getting last processed tick")
			}

			for _, tickInterval := range tickIntervals {
				if tickInterval.To > lastProcessedTick {
					// ok process
					start := max(tickInterval.From, lastProcessedTick+1)
					end := tickInterval.To
					return start, end, searchEpoch, nil
				}
			}
			// everything synced in this epoch
			searchEpoch++

		}
	}

	// no delta found do not sync
	return 0, 0, 0, nil

}
