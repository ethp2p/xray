package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/ethp2p/xray/eth"
	"github.com/ethp2p/xray/backend"
)

func main() {
	var (
		ingestAddr     = flag.String("ingest", "/tmp/xray.sock", "Address for the ingest listener (Unix path or host:port)")
		listenAddr     = flag.String("listen", "127.0.0.1:9100", "HTTP listen address for the dashboard API")
		genesisUnix    = flag.Int64("genesis-unix", 1606824023, "Beacon chain genesis unix timestamp")
		secondsPerSlot = flag.Uint64("seconds-per-slot", 12, "Beacon chain seconds per slot")
		dataDir        = flag.String("data-dir", defaultDataDir(), "Persistence directory for slot data")
		retentionDays  = flag.Int("retention-days", 30, "Slot retention period in days")
		staticDir      = flag.String("static-dir", "", "Serve dashboard static files from this directory")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	clock := eth.NewSlotClock(time.Unix(*genesisUnix, 0), *secondsPerSlot)
	processor := backend.NewProcessor(clock)
	registry := backend.NewSourceRegistry()

	storage, err := backend.NewStorage(*dataDir)
	if err != nil {
		log.Fatalf("create storage: %v", err)
	}

	metas, err := storage.LoadSourceMetas()
	if err != nil {
		log.Printf("load source metas: %v", err)
	}
	for _, meta := range metas {
		registry.Register(meta)
		if err := storage.LoadSummaryIndex(meta.SourceID); err != nil {
			log.Printf("load summary index for %s: %v", meta.SourceID, err)
		}
	}

	finalizeCh := make(chan backend.FinalizedSlot, 64)
	processor.SetOnFinalize(func(sourceID string, detail backend.SlotDetail) {
		select {
		case finalizeCh <- backend.FinalizedSlot{SourceID: sourceID, Detail: detail}:
		case <-ctx.Done():
		}
	})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case item := <-finalizeCh:
				if err := storage.WriteSlot(item.SourceID, item.Detail); err != nil {
					log.Printf("persist slot %d: %v", item.Detail.Summary.Slot, err)
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := storage.Prune(*retentionDays, 32, *secondsPerSlot, *genesisUnix); err != nil {
					log.Printf("retention prune: %v", err)
				}
			}
		}
	}()

	ingestListener := backend.NewIngestListener(processor, registry, storage)

	go func() {
		if err := ingestListener.ListenAndServe(ctx, *ingestAddr); err != nil && ctx.Err() == nil {
			log.Fatalf("ingest listener failed: %v", err)
		}
	}()

	server := backend.NewServer(processor, registry, storage)
	if *staticDir != "" {
		server.SetStaticDir(*staticDir)
	}
	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}

	log.Printf("introspector listening on http://%s (ingest on %s)", listener.Addr().String(), *ingestAddr)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func defaultDataDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".xray", "data")
}
