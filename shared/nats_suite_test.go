package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"

	"go.blockdaemon.com/aegis-event-streaming/pkg/servers/webhook"
)

func TestNATSSuite(t *testing.T) {
	suite.Run(t, new(NATSSuite))
}

// NATSSuite provides a shared NATS server for all tests
type NATSSuite struct {
	suite.Suite

	// shared
	natsServer        *server.Server
	natsServerOptions *server.Options
	natsServerURL     string

	// mock responder
	natsClient       *nats.Conn
	natsSubscription *nats.Subscription // always a new subscription for each test

	// internal states of the mock address handlers
	addresses         []common.Address
	contractAddresses []common.Address

	lock sync.Mutex

	timeout time.Duration
}

func (s *NATSSuite) TestCloseNATS() {
	s.initNATSWithHandlers([]AddressIdentifier{})

	err := CloseNATS()
	s.Require().NoError(err)

	s.Require().False(isNATSConnected())
	s.Require().False(hasNATSSubscription())
}

func (s *NATSSuite) TestInitNATS_Error() {
	s.setupAddressListResponder(
		[]AddressIdentifier{
			{Protocol: "ethereum", Network: "mainnet", Address: "0x1234567890123456789012345678901234567890"},
		},
	)

	// Initialize NATS with error handlers
	addressHandler := func(_ common.Address) error {
		return errors.New("")
	}
	err := InitNATS(
		s.Suite.T().Context(),
		&NATSOptions{URL: s.natsServerURL},
		addressHandler,
		addressHandler,
	)
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "failed to add address to monitored addresses")
}

func (s *NATSSuite) TestInitNATS_OK() {
	addressIdentifiers := []AddressIdentifier{
		{Protocol: "ethereum", Network: "mainnet", Address: "0x1234567890123456789012345678901234567890"},
		{Protocol: "ethereum", Network: "mainnet", Address: "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"},
	}
	s.initNATSWithHandlers(addressIdentifiers)

	s.lock.Lock()
	defer s.lock.Unlock()
	s.Require().Len(s.addresses, 2)
	s.Require().Len(s.contractAddresses, 2)
	s.Require().Contains(s.addresses, common.HexToAddress("0x1234567890123456789012345678901234567890"))
	s.Require().Contains(s.addresses, common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"))
}

func (s *NATSSuite) TestPublishSerializable_Concurrent() {
	var (
		receivedMessages      []webhook.WebhookMessageUnifiedConfirmedTxLogRequest
		receivedMessagesMutex sync.Mutex
	)
	_, err := s.natsClient.Subscribe(
		SubjectTransfer,
		func(msg *nats.Msg) {
			receivedMessagesMutex.Lock()
			defer receivedMessagesMutex.Unlock()

			var receivedData webhook.WebhookMessageUnifiedConfirmedTxLogRequest
			if err := json.Unmarshal(msg.Data, &receivedData); err != nil {
				s.T().Logf("Failed to unmarshal message in message handler: %v", err)
				return
			}
			receivedMessages = append(receivedMessages, receivedData)
		},
	)
	s.Require().NoError(err)

	s.initNATSWithHandlers([]AddressIdentifier{})

	var (
		group         errgroup.Group
		transactionID atomic.Int64
	)
	for range 5 {
		group.Go(
			func() error {
				for range 5 {
					var (
						txID = fmt.Sprintf("%d", transactionID.Add(1))
						data = webhook.WebhookMessageUnifiedConfirmedTxLogRequest{
							Data: webhook.WebhookMessageUnifiedConfirmedTxLogData{
								TxId: txID,
							},
						}
					)
					if err := PublishSerializable(data); err != nil {
						return fmt.Errorf("failed to publish message %s: %w", txID, err)
					}
				}
				return nil
			},
		)
	}
	err = group.Wait()
	s.Require().NoError(err)

	// Wait for all messages to be received
	expectedCount := int(transactionID.Load())
	s.waitForCondition(
		func() bool {
			receivedMessagesMutex.Lock()
			defer receivedMessagesMutex.Unlock()
			return len(receivedMessages) == expectedCount
		},
		"all messages should be received",
	)

	// Validate message content
	receivedMessagesMutex.Lock()
	defer receivedMessagesMutex.Unlock()

	transactionIDSet := make(map[string]bool)
	for _, msg := range receivedMessages {
		transactionIDSet[msg.Data.TxId] = true
	}

	for i := range expectedCount {
		expectedID := fmt.Sprintf("%d", i+1)
		s.Require().True(transactionIDSet[expectedID])
	}
}

func (s *NATSSuite) TestPublishSerializable_OK() {
	var (
		dataReceived webhook.WebhookMessageUnifiedConfirmedTxLogRequest
		lock         sync.Mutex
	)

	_, err := s.natsClient.Subscribe(
		SubjectTransfer,
		func(msg *nats.Msg) {
			lock.Lock()
			defer lock.Unlock()
			if err := json.Unmarshal(msg.Data, &dataReceived); err != nil {
				s.T().Logf("Failed to unmarshal message in message handler: %v", err)
				return
			}
		},
	)
	s.Require().NoError(err)

	s.initNATSWithHandlers([]AddressIdentifier{})

	// Load real test data from JSON file
	dataBytes, err := os.ReadFile("testdata/unified_confirmed_tx_log.json")
	s.Require().NoError(err)

	var data webhook.WebhookMessageUnifiedConfirmedTxLogRequest
	err = json.Unmarshal(dataBytes, &data)
	s.Require().NoError(err)

	err = PublishSerializable(data)
	s.Require().NoError(err)

	// Wait for message to be received and processed
	s.waitForCondition(func() bool {
		lock.Lock()
		defer lock.Unlock()
		return dataReceived.Data.TxId != "" // Check if message was received
	}, "message should be received")

	lock.Lock()
	defer lock.Unlock()
	s.Require().Equal(data, dataReceived)
}

func (s *NATSSuite) TestReconnect() {
	s.initNATSWithHandlers([]AddressIdentifier{})

	// Test initial messaging
	addressIdentifier := AddressIdentifier{
		Protocol: "ethereum",
		Network:  "mainnet",
		Address:  "0x1234567890123456789012345678901234567890",
	}
	addressIdentifierBytes, _ := json.Marshal(addressIdentifier)
	err := s.natsClient.Publish(SubjectAddressPublish, addressIdentifierBytes)
	s.Require().NoError(err)

	// Wait for the first message to be processed
	s.waitForCondition(
		func() bool {
			s.lock.Lock()
			defer s.lock.Unlock()
			return len(s.addresses) == 1 && len(s.contractAddresses) == 1
		},
		"first message should be processed",
	)

	s.natsServer.Shutdown()

	// Wait for disconnection to be detected
	s.waitForCondition(
		func() bool {
			return !isNATSConnected()
		},
		"should detect disconnection",
	)

	// Restart server
	server, err := server.NewServer(s.natsServerOptions)
	s.Require().NoError(err)

	go server.Start()

	s.Require().True(server.ReadyForConnections(s.timeout))
	s.natsServer = server

	// Wait for connection and subscription
	s.waitForCondition(
		func() bool {
			return isNATSConnected() && hasNATSSubscription()
		},
		"should connect and subscribe",
	)

	// Test messaging after reconnection
	err = s.natsClient.Publish(SubjectAddressPublish, addressIdentifierBytes)
	s.Require().NoError(err)

	// Wait for the second message to be processed
	s.waitForCondition(
		func() bool {
			s.lock.Lock()
			defer s.lock.Unlock()
			return len(s.addresses) == 2 && len(s.contractAddresses) == 2
		},
		"second message should be processed after reconnection",
	)
}

// SetupSuite runs once before all tests in the suite
func (s *NATSSuite) SetupSuite() {
	s.timeout = 5 * time.Second

	s.natsServerOptions = &server.Options{
		Host:   "127.0.0.1",
		Port:   -1, // random available port
		NoLog:  true,
		NoSigs: true,
	}
	server, err := server.NewServer(s.natsServerOptions)
	s.Require().NoError(err)
	go server.Start()
	s.Require().True(server.ReadyForConnections(s.timeout))
	s.natsServer = server
	s.natsServerURL = server.ClientURL()

	// Create client of the responder
	s.natsClient, err = nats.Connect(
		s.natsServerURL,
		nats.MaxReconnects(-1), // infinite retries
	)
	s.Require().NoError(err)
}

// SetupTest runs before each individual test
func (s *NATSSuite) SetupTest() {
	s.resetPackageVariables()

	// Reset tracked addresses
	s.addresses = nil
	s.contractAddresses = nil

	// Clean up any previous nats subscription
	if s.natsSubscription != nil && s.natsSubscription.IsValid() {
		s.natsSubscription.Unsubscribe()
		s.natsSubscription = nil
	}
}

// TearDownSuite runs once after all tests in the suite
func (s *NATSSuite) TearDownSuite() {
	if s.natsClient != nil {
		s.natsClient.Close()
	}
	if s.natsServer != nil {
		s.natsServer.Shutdown()
	}
}

// Helper method to initialize NATS with standard setup
func (s *NATSSuite) initNATSWithHandlers(addresses []AddressIdentifier) {
	s.setupAddressListResponder(addresses)

	options := &NATSOptions{URL: s.natsServerURL}

	addressHandler := func(address common.Address) error {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.addresses = append(s.addresses, address)
		return nil
	}

	contractAddressHandler := func(address common.Address) error {
		s.lock.Lock()
		defer s.lock.Unlock()
		s.contractAddresses = append(s.contractAddresses, address)
		return nil
	}

	err := InitNATS(
		s.Suite.T().Context(),
		options,
		addressHandler,
		contractAddressHandler,
	)
	s.Require().NoError(err)

	// Wait for connection and subscription to be established
	s.waitForCondition(func() bool {
		return isNATSConnected() && hasNATSSubscription()
	}, "NATS should connect and subscribe")
}

func (s *NATSSuite) resetPackageVariables() {
	natsMutex.Lock()
	defer natsMutex.Unlock()
	err := cleanupSubscription()
	s.Require().NoError(err)

	if natsConn != nil {
		natsConn.Close()
		natsConn = nil
	}

	if stopLoggingFunc != nil {
		stopLoggingFunc()
		stopLoggingFunc = nil
	}

	natsOptions = nil
	addressHandler = nil
	contractAddressHandler = nil
}

// setupAddressListResponder sets up a NATS subscription to respond to address list requests with the given addresses
func (s *NATSSuite) setupAddressListResponder(addresses []AddressIdentifier) {
	natsSubscription, err := s.natsClient.Subscribe(
		SubjectAddressListRequest,
		func(msg *nats.Msg) {
			response, err := json.Marshal(addresses)
			if err != nil {
				s.T().Logf("Failed to marshal address list in nats message handler: %v", err)
				return
			}
			if err := msg.Respond(response); err != nil {
				s.T().Logf("Failed to respond in nats message handler: %v", err)
				return
			}
		},
	)
	s.Require().NoError(err)
	s.natsSubscription = natsSubscription
}

// waitForCondition polls a condition with timeout
func (s *NATSSuite) waitForCondition(condition func() bool, expectation string) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.NewTimer(s.timeout)
	defer timeout.Stop()

	for {
		select {
		case <-ticker.C:
			if condition() {
				return
			}
		case <-timeout.C:
			s.Require().Fail("timeout waiting for condition: " + expectation)
		}
	}
}

// hasNATSSubscription checks if NATS subscription is valid
func hasNATSSubscription() bool {
	natsMutex.RLock()
	defer natsMutex.RUnlock()

	if subscription == nil {
		return false
	}
	return subscription.IsValid()
}
