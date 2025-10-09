package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/nats-io/nats.go"
)

const (
	SubjectAddressListRequest = "evm.tracker.address.list.request"
	SubjectAddressPublish     = "evm.tracker.address.publish"
	SubjectTransfer           = "evm.tracker.transfer"
)

var (
	natsConn        *nats.Conn
	natsMutex       sync.RWMutex
	natsOptions     *NATSOptions
	subscription    *nats.Subscription
	addressHandler  AddressHandler
	stopLoggingFunc func()
)

type (
	AddressIdentifier struct {
		Protocol string `json:"protocol"`
		Network  string `json:"network"`
		Address  string `json:"address"`
	}

	AddressHandler func(addr common.Address) error
)

// CloseNATS closes the NATS connection
func CloseNATS() error {
	natsMutex.Lock()
	defer natsMutex.Unlock()

	if stopLoggingFunc != nil {
		stopLoggingFunc()
		stopLoggingFunc = nil
	}

	if err := cleanupSubscription(); err != nil {
		slog.Warn("Failed to cleanup NATS subscription", "error", err)
	}

	if natsConn != nil {
		if err := natsConn.Drain(); err != nil {
			slog.Warn("Failed to drain NATS connection", "error", err)
		}

		natsConn.Close()
		natsConn = nil
	}

	return nil
}

// InitNATS initializes the NATS connection
func InitNATS(
	ctx context.Context,
	options *NATSOptions,
	addressHandlerInput AddressHandler,
) error {
	natsMutex.Lock()
	defer natsMutex.Unlock()

	if natsConn != nil {
		return errors.New("NATS connection already initiated")
	}

	if options == nil {
		return errors.New("empty NATS client options")
	}

	if addressHandler != nil {
		return errors.New("address handler already set")
	}
	addressHandler = addressHandlerInput

	natsOptions = options

	if err := connectToNATS(); err != nil {
		return err
	}

	if err := requestAddressList(ctx); err != nil {
		return fmt.Errorf("failed to request addresses: %w", err)
	}

	return nil
}

// PublishSerializable publishes serializable data to NATS
func PublishSerializable(data any) error {
	if !isNATSConnected() {
		return errors.New("not connected to nats server")
	}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook data: %w", err)
	}

	// Publish to NATS with subject
	if err = natsConn.Publish(SubjectTransfer, jsonData); err != nil {
		return fmt.Errorf("failed to publish to NATS: %w", err)
	}

	// Optionally flush to ensure delivery
	if err := natsConn.Flush(); err != nil {
		return fmt.Errorf("failed to flush NATS connection: %w", err)
	}

	// Log publish event
	slog.Info("Published to NATS", "subject", SubjectTransfer, "bytes", len(jsonData))

	return nil
}

// cleanupSubscription cleans up the NATS subscription.
func cleanupSubscription() error {
	if subscription != nil {
		if err := subscription.Unsubscribe(); err != nil {
			return fmt.Errorf("failed to unsubscribe from NATS subject %s: %w", SubjectAddressPublish, err)
		}

		if err := subscription.Drain(); err != nil {
			return fmt.Errorf("failed to drain NATS subscription: %w", err)
		}

		subscription = nil
	}

	return nil
}

// connectToNATS establishes a connection to NATS with robust reconnection options
func connectToNATS() error {
	if natsOptions == nil {
		return errors.New("NATS options not set")
	}

	// Stop existing stats logging
	if stopLoggingFunc != nil {
		stopLoggingFunc()
		stopLoggingFunc = nil
	}

	// Close existing connection if any
	if natsConn != nil {
		natsConn.Close()
		natsConn = nil
	}

	disconnectedHandler := func(c *nats.Conn, err error) {
		servers := c.Servers()
		slog.Info("Disconnected from NATS servers", "error", err, "servers", servers)
	}
	reconnectHandler := func(c *nats.Conn) {
		natsMutex.Lock()
		defer natsMutex.Unlock()

		slog.Info("Reconnected to NATS", "url", c.ConnectedUrlRedacted())
		if err := subscribeToAddress(); err != nil {
			slog.Error("Failed to reestablish subscription after reconnect", "error", err)
		}
	}
	connectHandler := func(c *nats.Conn) {
		natsMutex.Lock()
		defer natsMutex.Unlock()

		slog.Info("Connected to NATS", "url", c.ConnectedUrlRedacted())
		if err := subscribeToAddress(); err != nil {
			slog.Error("Failed to establish subscription after connect", "error", err)
		}
	}

	var err error
	natsConn, err = nats.Connect(
		natsOptions.URL,
		nats.UserInfo(natsOptions.User, natsOptions.Password),
		nats.Timeout(10*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),            // infinite retries
		nats.ReconnectWait(1*time.Second), // wait time between reconnection attempts
		nats.DisconnectErrHandler(disconnectedHandler),
		nats.ReconnectHandler(reconnectHandler),
		nats.ConnectHandler(connectHandler),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Start logging statistics
	stopLoggingFunc = logStats(natsConn, 1*time.Minute)

	slog.Info("Connected to NATS server", "url", natsOptions.URL)
	return nil
}

// isNATSConnected checks if NATS connection is active and connected
func isNATSConnected() bool {
	if natsConn == nil {
		return false
	}
	return natsConn.IsConnected()
}

// logStats logs NATS connection statistics periodically
func logStats(natsConn *nats.Conn, interval time.Duration) func() {
	var (
		stop = make(chan struct{})
		once sync.Once
	)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if natsConn != nil && natsConn.IsConnected() {
					stats := natsConn.Stats()
					slog.Info(
						"NATS Connection Stats",
						"url", natsConn.ConnectedUrlRedacted(),
						"in_msgs", stats.InMsgs,
						"out_msgs", stats.OutMsgs,
						"in_bytes", stats.InBytes,
						"out_bytes", stats.OutBytes,
						"reconnects", stats.Reconnects,
					)
				}
			case <-stop:
				return
			}
		}
	}()

	return func() {
		once.Do(
			func() {
				close(stop)
			},
		)
	}
}

// requestAddressList requests the latest list of addresses from NATS and adds them to the monitored addresses
func requestAddressList(ctx context.Context) error {
	if !isNATSConnected() {
		return errors.New("not connected to nats server")
	}

	// Prepare request payload including desired chain hints
	type addressListQuery struct {
		ChainID int64 `json:"chain_id,omitempty"`
	}

	query := addressListQuery{}
	if ChainID != nil {
		query.ChainID = ChainID.Int64()
	}

	payload, _ := json.Marshal(query)

	// Backoff-and-retry when there are no responders
	const (
		maxAttempts      = 5
		initialBackoffMs = 500
	)
	backoff := time.Duration(initialBackoffMs) * time.Millisecond
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Send request with timeout context
		msg, err := natsConn.RequestWithContext(ctx, SubjectAddressListRequest, payload)
		if err == nil {
			// Process the response
			var addresses []AddressIdentifier
			if unmarshalErr := json.Unmarshal(msg.Data, &addresses); unmarshalErr != nil {
				return fmt.Errorf("failed to unmarshal addresses: %w", unmarshalErr)
			}

			slog.Info("Received addresses from NATS", "addresses", addresses)

			for _, address := range addresses {
				if handleErr := addressHandler(common.HexToAddress(address.Address)); handleErr != nil {
					return fmt.Errorf("failed to add address to monitored addresses: %w", handleErr)
				}
			}
			return nil
		}

		lastErr = err
		// Retry only on no responders, otherwise return immediately
		if errors.Is(err, nats.ErrNoResponders) || (err != nil && strings.Contains(err.Error(), "no responders")) {
			slog.Warn("No NATS responders, backing off", "attempt", attempt, "max", maxAttempts, "backoff", backoff)
			// Wait with context awareness
			select {
			case <-time.After(backoff):
				backoff *= 2
				continue
			case <-ctx.Done():
				return fmt.Errorf("context cancelled while waiting to retry NATS request: %w", ctx.Err())
			}
		}

		return fmt.Errorf("failed to send request to NATS: %w", err)
	}

	return fmt.Errorf("failed to send request to NATS after retries: %w", lastErr)
}

// subscribeToAddress subscribes to address addition and adds it to the monitored addresses.
// This function assumes the NATS connection is already established.
func subscribeToAddress() error {
	// Always clean up existing subscription first
	if err := cleanupSubscription(); err != nil {
		slog.Warn("Failed to cleanup existing subscription", "error", err)
	}

	newSubscription, err := natsConn.Subscribe(
		SubjectAddressPublish,
		func(msg *nats.Msg) {
			var address AddressIdentifier
			if err := json.Unmarshal(msg.Data, &address); err != nil {
				slog.Error("failed to unmarshal address", "error", err)
				return
			}

			// Always print to console when a wallet address is published via NATS
			fmt.Printf("Published wallet address received: %s (protocol=%s, network=%s)\n", address.Address, address.Protocol, address.Network)

			if err := addressHandler(common.HexToAddress(address.Address)); err != nil {
				slog.Error("handling address", "error", err)
				return
			}

			slog.Info(
				"Monitored address registered",
				"protocol", address.Protocol,
				"network", address.Network,
				"address", address.Address,
			)
		},
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS subject %s: %w", SubjectAddressPublish, err)
	}

	subscription = newSubscription

	return nil
}
