package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/nats-io/nats.go"
)

const (
	subjectAddress  = "evm.tracker.address"
	subjectTransfer = "evm.tracker.transfer"
)

var (
	natsConn *nats.Conn
)

type Address struct {
	L1Blockchain string
	Network      string
	Address      string
}

type NatsConfig struct {
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"password"`
}

// InitNATS initializes the NATS connection
func InitNATS(config *NatsConfig) error {
	var err error
	natsConn, err = nats.Connect(
		config.URL,
		nats.UserInfo(config.User, config.Password),
		nats.Timeout(10*time.Second),    // Add a longer timeout
		nats.RetryOnFailedConnect(true), // Retry connection
		nats.MaxReconnects(5),           // Set max reconnection attempts
	)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %v", err)
	}
	slog.Info("Connected to NATS server", "url", config.URL)

	return nil
}

// PublishTransfer publishes transfer data to NATS
func PublishTransfer(data any) error {
	if natsConn == nil {
		return fmt.Errorf("NATS connection not initialized")
	}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook data: %v", err)
	}

	// Publish to NATS with subject
	if err = natsConn.Publish(subjectTransfer, jsonData); err != nil {
		return fmt.Errorf("failed to publish to NATS: %v", err)
	}

	// Optionally flush to ensure delivery
	if err := natsConn.Flush(); err != nil {
		return fmt.Errorf("failed to flush NATS connection: %v", err)
	}

	return nil
}

func SubscribeToAddressEvents(ctx context.Context, addressChannel chan<- common.Address) error {
	subscription, err := natsConn.Subscribe(
		subjectAddress,
		func(msg *nats.Msg) {
			var address Address
			if err := json.Unmarshal(msg.Data, &address); err != nil {
				slog.Error("failed to unmarshal address", "error", err)
				return
			}
			slog.Info("Received address event", "address", address)

			addressChannel <- common.HexToAddress(address.Address)
		},
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS subject %s: %w", subjectAddress, err)
	}
	defer subscription.Unsubscribe()

	// Wait for context to be done
	<-ctx.Done()

	return nil
}
