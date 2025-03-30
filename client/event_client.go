package client

import (
	"context"
	"github.com/pkg/errors"
	eventspb "github.com/qubic/go-events/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type IntegrationEventClient struct {
	eventApi eventspb.EventsServiceClient
}

type TickInfo struct {
	CurrentTick uint32
	InitialTick uint32
}

type EventStatus struct {
	Epoch     uint32
	Tick      uint32
	Intervals map[uint32][]*ProcessedTickInterval
}

type ProcessedTickInterval struct {
	From uint32
	To   uint32
}

func NewIntegrationEventClient(eventApiUrl string) (*IntegrationEventClient, error) {
	eventApiConn, err := grpc.NewClient(eventApiUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Wrap(err, "creating event api connection")
	}
	e := IntegrationEventClient{
		eventApi: eventspb.NewEventsServiceClient(eventApiConn),
	}
	return &e, nil
}

func (eventClient *IntegrationEventClient) GetEvents(context context.Context, tickNumber uint32) (*eventspb.TickEvents, error) {
	return eventClient.eventApi.GetTickEvents(context, &eventspb.GetTickEventsRequest{Tick: tickNumber})
}

func (eventClient *IntegrationEventClient) GetStatus(context context.Context) (*EventStatus, error) {
	s, err := eventClient.eventApi.GetStatus(context, nil)
	if err != nil {
		return nil, errors.Wrap(err, "getting event status")
	}
	intervals := map[uint32][]*ProcessedTickInterval{}
	for _, epochIntervals := range s.GetProcessedTickIntervalsPerEpoch() {
		processingIntervals := make([]*ProcessedTickInterval, 0)
		for _, interval := range epochIntervals.Intervals {
			processingIntervals = append(processingIntervals, &ProcessedTickInterval{
				From: interval.InitialProcessedTick,
				To:   interval.LastProcessedTick,
			})
		}
		intervals[epochIntervals.Epoch] = processingIntervals
	}
	status := EventStatus{
		Tick:      s.GetLastProcessedTick().GetTickNumber(),
		Epoch:     s.GetLastProcessedTick().GetEpoch(),
		Intervals: intervals,
	}
	return &status, nil
}
