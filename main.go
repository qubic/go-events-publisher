package main

import (
	"github.com/pkg/errors"
	"log"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	return errors.New("main: please implement me")
}
