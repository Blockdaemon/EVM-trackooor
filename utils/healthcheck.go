package utils

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/Zellic/EVM-trackooor/shared"
)

// HealthServer contains the HTTP server for health checks
type HealthServer struct {
	server *http.Server
}

// NewHealthServer creates a new health check server
func NewHealthServer(port string) *HealthServer {
	mux := http.NewServeMux()

	// Define health check endpoints
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// LivenessProbe - Checks if the server is running
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// ReadinessProbe - Checks if the server is ready to accept requests
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// Check if connected to RPC endpoint
		if shared.Client == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not connected to RPC"))
			return
		}

		// Try to get the latest block number to verify connectivity
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := shared.Client.BlockNumber(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Failed to get latest block: " + err.Error()))
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	return &HealthServer{
		server: &http.Server{
			Addr:    ":" + port,
			Handler: mux,
		},
	}
}

// Start begins the health check server
func (h *HealthServer) Start() {
	shared.Infof(slog.Default(), "Starting health check server on %s\n", h.server.Addr)
	go func() {
		if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			shared.Infof(slog.Default(), "Health check server error: %v\n", err)
		}
	}()
}

// Shutdown gracefully stops the health check server
func (h *HealthServer) Shutdown(ctx context.Context) error {
	shared.Infof(slog.Default(), "Shutting down health check server\n")
	return h.server.Shutdown(ctx)
}
