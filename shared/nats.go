package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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
	natsConn       *nats.Conn
	subscription   *nats.Subscription
	addressHandler AddressHandler
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
	if subscription != nil {
		if err := subscription.Unsubscribe(); err != nil {
			return fmt.Errorf("failed to unsubscribe from NATS: %w", err)
		}
	}
	if natsConn != nil {
		if err := natsConn.Drain(); err != nil {
			return fmt.Errorf("failed to drain NATS connection: %w", err)
		}
		natsConn.Close()
	}
	return nil
}

// InitNATS initializes the NATS connection
func InitNATS(ctx context.Context, options *NATSOptions, handler AddressHandler) error {
	if natsConn != nil {
		return errors.New("NATS connection already initiated")
	}

	if options == nil {
		return errors.New("empty NATS client options")
	}

	if addressHandler != nil {
		return errors.New("address handler already set")
	}
	addressHandler = handler

	var err error
	natsConn, err = nats.Connect(
		options.URL,
		nats.UserInfo(options.User, options.Password),
		nats.Timeout(10*time.Second),    // Add a longer timeout
		nats.RetryOnFailedConnect(true), // Retry connection
		nats.MaxReconnects(5),           // Set max reconnection attempts
	)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	slog.Info("Connected to NATS server", "url", options.URL)

	if err := SubscribeToAddress(); err != nil {
		return fmt.Errorf("failed to subscribe to address events: %w", err)
	}

	if err := RequestAddressList(ctx); err != nil {
		return fmt.Errorf("failed to request addresses: %w", err)
	}

	return nil
}

// PublishSerializable publishes serializable data to NATS
func PublishSerializable(data any) error {
	if natsConn == nil {
		return fmt.Errorf("NATS connection not initialized")
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

	return nil
}

// RequestAddressList requests the latest list of addresses from NATS and adds them to the monitored addresses
func RequestAddressList(ctx context.Context) error {
	if natsConn == nil {
		return fmt.Errorf("NATS connection not initialized")
	}

	// Send request with timeout context
	msg, err := natsConn.RequestWithContext(ctx, SubjectAddressListRequest, nil)
	if err != nil {
		return fmt.Errorf("failed to send request to NATS: %w", err)
	}

	// Process the response
	var addresses []AddressIdentifier
	if err := json.Unmarshal(msg.Data, &addresses); err != nil {
		return fmt.Errorf("failed to unmarshal addresses: %w", err)
	}

	slog.Info("Received addresses from NATS", "addresses", addresses)

	for _, address := range addresses {
		if err := addressHandler(common.HexToAddress(address.Address)); err != nil {
			return fmt.Errorf("failed to add address to monitored addresses: %w", err)
		}
	}

	return nil
}

// SubscribeToAddress subscribes to address addition and adds it to the monitored addresses
func SubscribeToAddress() error {
	if natsConn == nil {
		return fmt.Errorf("NATS connection not initialized")
	}

	var err error
	subscription, err = natsConn.Subscribe(
		SubjectAddressPublish,
		func(msg *nats.Msg) {
			var address AddressIdentifier
			if err := json.Unmarshal(msg.Data, &address); err != nil {
				slog.Error("failed to unmarshal address", "error", err)
				return
			}

			if err := addressHandler(common.HexToAddress(address.Address)); err != nil {
				slog.Error("handling address", "error", err)
				return
			}

			return
		},
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS subject %s: %w", SubjectAddressPublish, err)
	}

	return nil
}
