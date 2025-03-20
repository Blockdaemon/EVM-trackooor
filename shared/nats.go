package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/nats-io/nats.go"
)

const (
	SubjectAddressListRequest = "evm.tracker.address.list.request"
	SubjectAddressPublish     = "evm.tracker.address.publish"
	SubjectTransfer           = "evm.tracker.transfer"
)

var (
	monitoredAddresses = mapset.NewSet[common.Address]()
	natsConn           *nats.Conn
	subscription       *nats.Subscription
	onAddressAdded     AddressAddedCallback
)

type Address struct {
	Protocol string `json:"protocol"`
	Network  string `json:"network"`
	Address  string `json:"address"`
}

type AddressAddedCallback func(addr common.Address)

type NatsConfig struct {
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"password"`
}

// AddMonitoredAddress adds an address to the monitored addresses
func AddMonitoredAddress(address common.Address) error {
	if onAddressAdded == nil {
		return fmt.Errorf("no address added callback set")
	}

	monitoredAddresses.Add(address)
	onAddressAdded(address)

	slog.Info("Added monitored address", "address", address.String())

	return nil
}

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
func InitNATS(ctx context.Context, config *NatsConfig) error {
	var err error
	natsConn, err = nats.Connect(
		config.URL,
		nats.UserInfo(config.User, config.Password),
		nats.Timeout(10*time.Second),    // Add a longer timeout
		nats.RetryOnFailedConnect(true), // Retry connection
		nats.MaxReconnects(5),           // Set max reconnection attempts
	)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	slog.Info("Connected to NATS server", "url", config.URL)

	if err := SubscribeToAddress(); err != nil {
		return fmt.Errorf("failed to subscribe to address events: %w", err)
	}

	if err := RequestAddressList(ctx); err != nil {
		return fmt.Errorf("failed to request addresses: %w", err)
	}

	return nil
}

func MonitoredAddressesContain(addr common.Address) bool {
	return monitoredAddresses.Contains(addr)
}

// PublishTransfer publishes transfer data to NATS
func PublishTransfer(data any) error {
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
	var addresses []Address
	if err := json.Unmarshal(msg.Data, &addresses); err != nil {
		return fmt.Errorf("failed to unmarshal addresses: %w", err)
	}

	for _, address := range addresses {
		AddMonitoredAddress(common.HexToAddress(address.Address))
	}

	return nil
}

// SetAddressAddedCallback sets the callback function to be called when a new address is added
func SetAddressAddedCallback(callback AddressAddedCallback) {
	onAddressAdded = callback
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
			var address Address
			if err := json.Unmarshal(msg.Data, &address); err != nil {
				slog.Error("failed to unmarshal address", "error", err)
				return
			}

			AddMonitoredAddress(common.HexToAddress(address.Address))
		},
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to NATS subject %s: %w", SubjectAddressPublish, err)
	}

	return nil
}
