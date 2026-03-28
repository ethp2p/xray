package main

import (
	"flag"
	"log"
	"net"
	"time"

	"github.com/ethp2p/instrument/eth"
	"github.com/ethp2p/instrument/introspector"
)

func main() {
	var (
		socketPath     = flag.String("socket", "/tmp/wiretap-introspector.sock", "Unix socket path exposed by the wiretap")
		listenAddr     = flag.String("listen", "127.0.0.1:9100", "HTTP listen address for the dashboard API")
		genesisUnix    = flag.Int64("genesis-unix", 1606824023, "Beacon chain genesis unix timestamp")
		secondsPerSlot = flag.Uint64("seconds-per-slot", 12, "Beacon chain seconds per slot")
	)
	flag.Parse()

	clock := eth.NewSlotClock(time.Unix(*genesisUnix, 0), *secondsPerSlot)
	processor := introspector.NewProcessor(clock)
	registry := introspector.NewSourceRegistry()
	ingestClient := introspector.NewUnixIngestClient(*socketPath, processor, "default")
	server := introspector.NewServer(processor, registry, nil)

	go func() {
		if err := ingestClient.Run(); err != nil {
			log.Fatalf("ingest client failed: %v", err)
		}
	}()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	log.Printf("introspector listening on http://%s", listener.Addr().String())
	if err := server.Serve(listener); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
