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
	eventClient    EventClient
	eventProcessor EventProcessor
	dataStore      DataStore
}

func NewEventReader(client EventClient, processor EventProcessor, store DataStore) *EventReader {
	es := EventReader{
		eventClient:    client,
		eventProcessor: processor,
		dataStore:      store,
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
			log.Printf("sync run failed: %v", err)
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
		err = r.processTickEventsRange(ctx, epoch, start, end+1) // end exclusive
		if err != nil {
			return startEpoch, errors.Wrapf(err, "processing tick range from [%d] to [%d]", start, end)
		}
	}

	return epoch, nil
}

func (r *EventReader) processTickEventsRange(ctx context.Context, epoch, from, toExcl uint32) error {
	for tick := from; tick < toExcl; tick++ {
		err := r.processTickEvents(ctx, tick)
		if err != nil {
			return errors.Wrapf(err, "processing tick [%d]", tick)
		}
		err = r.dataStore.SetLastProcessedTick(epoch, tick)
		if err != nil {
			return errors.Wrapf(err, "setting last processed tick [%d]", tick)
		}
	}
	return nil
}

func (r *EventReader) processTickEvents(ctx context.Context, tick uint32) error {

	tickEvents, err := r.eventClient.GetEvents(ctx, tick)
	if err != nil {
		return errors.Wrap(err, "getting events")
	}

	err = r.eventProcessor.ProcessTickEvents(ctx, tickEvents)
	if err != nil {
		return errors.Wrap(err, "processing events")
	}

	return nil
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
