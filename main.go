package main

import (
	"context"
	"kraken-data-collector/kraken"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := kraken.New()
	if err := s.Start(ctx); err != nil {
		logrus.Fatalf("start collector: %v", err)
	}
	s.Stop()
}
