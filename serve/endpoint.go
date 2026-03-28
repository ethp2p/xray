package serve

import (
	"net"
	"net/http"

	"connectrpc.com/connect"
	connectcors "connectrpc.com/cors"
	"github.com/rs/cors"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	instrument "github.com/ethp2p/instrument"
	pbconnect "github.com/ethp2p/instrument/pb/instrumentpbconnect"
)

// Endpoint serves the Connect API and WebSocket for client access.
type Endpoint struct {
	httpServer *http.Server
}

// NewEndpoint creates a new endpoint server offering Connect, WebSocket, and CORS.
func NewEndpoint(service pbconnect.WiretapServiceHandler, addr string, emitter *instrument.Emitter) *Endpoint {
	mux := http.NewServeMux()

	path, handler := pbconnect.NewWiretapServiceHandler(
		service,
		connect.WithCompressMinBytes(1024),
	)
	mux.Handle(path, handler)
	mux.Handle("/ws", NewWsServer(emitter))

	corsHandler := withCORS(mux)
	h2s := &http2.Server{}

	return &Endpoint{
		httpServer: &http.Server{
			Addr:    addr,
			Handler: h2c.NewHandler(corsHandler, h2s),
		},
	}
}

func withCORS(h http.Handler) http.Handler {
	middleware := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: connectcors.AllowedMethods(),
		AllowedHeaders: connectcors.AllowedHeaders(),
		ExposedHeaders: connectcors.ExposedHeaders(),
	})
	return middleware.Handler(h)
}

// Serve starts the HTTP server on an existing listener.
func (e *Endpoint) Serve(l net.Listener) error {
	return e.httpServer.Serve(l)
}

// Close shuts down the endpoint server.
func (e *Endpoint) Close() error {
	return e.httpServer.Close()
}
