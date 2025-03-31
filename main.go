package main

import (
	"fmt"
	"github.com/ardanlabs/conf"
	"github.com/pkg/errors"
	"github.com/qubic/go-events-publisher/client"
	"github.com/qubic/go-events-publisher/sync"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const envPrefix = "QUBIC_EVENTS_PUBLISHER"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {

	log.SetOutput(os.Stdout) // default is stderr

	var cfg struct {
		Client struct {
			EventApiUrl string `conf:"required"`
		}
		Broker struct {
			BootstrapServers string `conf:"default:localhost:9092"`
		}
		Sync struct {
			InternalStoreFolder string `conf:"default:store"`
			StartEpoch          uint32 `conf:"default:153"`
		}
	}

	// load config
	if err := conf.Parse(os.Args[1:], envPrefix, &cfg); err != nil {
		switch {
		case errors.Is(err, conf.ErrHelpWanted):
			usage, err := conf.Usage(envPrefix, &cfg)
			if err != nil {
				return errors.Wrap(err, "generating config usage")
			}
			fmt.Println(usage)
			return nil
		case errors.Is(err, conf.ErrVersionWanted):
			version, err := conf.VersionString(envPrefix, &cfg)
			if err != nil {
				return errors.Wrap(err, "generating config version")
			}
			fmt.Println(version)
			return nil
		}
		return errors.Wrap(err, "parsing config")
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return errors.Wrap(err, "generating config for output")
	}
	log.Printf("main: Config :\n%v\n", out)

	eventClient, err := client.NewIntegrationEventClient(cfg.Client.EventApiUrl)
	if err != nil {
		return errors.Wrap(err, "creating event client")
	}

	store, err := sync.NewPebbleStore(cfg.Sync.InternalStoreFolder)
	if err != nil {
		return errors.Wrap(err, "creating db")
	}

	kcl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Broker.BootstrapServers),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer kcl.Close()

	eventProcessor := sync.NewEventPublisher(kcl)
	eventReader := sync.NewEventReader(eventClient, eventProcessor, store)
	go eventReader.SyncInLoop(cfg.Sync.StartEpoch)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	log.Println("main: Service started.")

	for {
		select {
		case <-shutdown:
			log.Println("main: Received shutdown signal, shutting down...")
			return nil
		}
	}

}
