//go:build !ci
// +build !ci

package client

import (
	"context"
	"flag"
	"github.com/ardanlabs/conf"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

var (
	eventClient *IntegrationEventClient
)

func TestEventClient_GetEvents(t *testing.T) {
	const tickNumber uint32 = 19236443 // needs current tick number
	tickEvents, err := eventClient.GetEvents(context.Background(), tickNumber)
	assert.Nil(t, err)
	log.Printf("Received tick [%d] events: %v", tickNumber, tickEvents)
}

func TestEventClient_GetStatus(t *testing.T) {
	status, err := eventClient.GetStatus(context.Background())
	assert.Nil(t, err)
	assert.NotNil(t, status.Tick, "last processed tick is nil")
	log.Printf("Received event status: %v", status)
}

func TestMain(m *testing.M) {
	// slog.SetLogLoggerLevel(slog.LevelDebug)
	setup()
	// Parse args and run
	flag.Parse()
	exitCode := m.Run()
	// Exit
	os.Exit(exitCode)
}

func setup() {
	const envPrefix = "QUBIC_EVENTS_PUBLISHER"
	err := godotenv.Load("../.env.local")
	if err != nil {
		log.Println("Using no env file")
	}
	var config struct {
		Client struct {
			EventApiUrl string `conf:"required"`
		}
	}
	err = conf.Parse(os.Args[1:], envPrefix, &config)
	if err != nil {
		log.Printf("error getting config: %v", err)
		os.Exit(-1)
	}
	eventClient, err = NewIntegrationEventClient(config.Client.EventApiUrl)
	if err != nil {
		log.Printf("error creating event client: %v", err)
		os.Exit(-1)
	}
}
