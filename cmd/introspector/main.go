package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/ethp2p/instrument/eth"
	"github.com/ethp2p/instrument/introspector"
)

func main() {
	var (
		ingestAddr     = flag.String("ingest", "/tmp/wiretap-introspector.sock", "Address for the ingest listener (Unix path or host:port)")
		listenAddr     = flag.String("listen", "127.0.0.1:9100", "HTTP listen address for the dashboard API")
		genesisUnix    = flag.Int64("genesis-unix", 1606824023, "Beacon chain genesis unix timestamp")
		secondsPerSlot = flag.Uint64("seconds-per-slot", 12, "Beacon chain seconds per slot")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	clock := eth.NewSlotClock(time.Unix(*genesisUnix, 0), *secondsPerSlot)
	processor := introspector.NewProcessor(clock)
	registry := introspector.NewSourceRegistry()
	ingestListener := introspector.NewIngestListener(processor, registry, nil)

	go func() {
		if err := ingestListener.ListenAndServe(ctx, *ingestAddr); err != nil && ctx.Err() == nil {
			log.Fatalf("ingest listener failed: %v", err)
		}
	}()

	server := introspector.NewServer(processor, registry, nil)
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	log.Printf("introspector listening on http://%s (ingest on %s)", listener.Addr().String(), *ingestAddr)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
