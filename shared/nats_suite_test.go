package shared

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
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

// mTLS Tests

func (s *NATSSuite) TestGetTLSConfig_NoMTLS() {
	options := &NATSOptions{
		URL:      "nats://localhost:4222",
		User:     "user",
		Password: "pass",
	}

	tlsConfig, err := getTLSConfig(options)
	s.Require().NoError(err)
	s.Require().Nil(tlsConfig)
}

func (s *NATSSuite) TestGetTLSConfig_WithMTLS() {
	tempDir, err := os.MkdirTemp("", "nats-mtls-test-*")
	s.Require().NoError(err)
	defer os.RemoveAll(tempDir)

	certPaths, err := generateAllCertificates(tempDir)
	s.Require().NoError(err)

	options := &NATSOptions{
		URL:                      "nats://localhost:4222",
		ClientCertificateFile:    certPaths.ClientCertFile,
		ClientCertificateKeyFile: certPaths.ClientKeyFile,
		CABundleFile:             certPaths.CAFile,
	}

	tlsConfig, err := getTLSConfig(options)
	s.Require().NoError(err)
	s.Require().NotNil(tlsConfig)
	s.Require().Equal(uint16(tls.VersionTLS12), tlsConfig.MinVersion)
	s.Require().Equal(uint16(tls.VersionTLS13), tlsConfig.MaxVersion)
	s.Require().NotEmpty(tlsConfig.Certificates)
	s.Require().NotNil(tlsConfig.RootCAs)
}

func (s *NATSSuite) TestGetTLSConfig_InvalidCertFile() {
	tempDir, err := os.MkdirTemp("", "nats-mtls-test-*")
	s.Require().NoError(err)
	defer os.RemoveAll(tempDir)

	certPaths, err := generateAllCertificates(tempDir)
	s.Require().NoError(err)

	options := &NATSOptions{
		URL:                      "nats://localhost:4222",
		ClientCertificateFile:    "/nonexistent/cert.pem",
		ClientCertificateKeyFile: certPaths.ClientKeyFile,
		CABundleFile:             certPaths.CAFile,
	}

	tlsConfig, err := getTLSConfig(options)
	s.Require().Error(err)
	s.Require().Nil(tlsConfig)
	s.Require().Contains(err.Error(), "failed to load client certificate")
}

func (s *NATSSuite) TestGetTLSConfig_InvalidCAFile() {
	tempDir, err := os.MkdirTemp("", "nats-mtls-test-*")
	s.Require().NoError(err)
	defer os.RemoveAll(tempDir)

	certPaths, err := generateAllCertificates(tempDir)
	s.Require().NoError(err)

	options := &NATSOptions{
		URL:                      "nats://localhost:4222",
		ClientCertificateFile:    certPaths.ClientCertFile,
		ClientCertificateKeyFile: certPaths.ClientKeyFile,
		CABundleFile:             "/nonexistent/ca.pem",
	}

	tlsConfig, err := getTLSConfig(options)
	s.Require().Error(err)
	s.Require().Nil(tlsConfig)
	s.Require().Contains(err.Error(), "failed to read CA bundle file")
}

func (s *NATSSuite) TestGetTLSConfig_PartialConfig_OnlyCA() {
	tempDir, err := os.MkdirTemp("", "nats-mtls-test-*")
	s.Require().NoError(err)
	defer os.RemoveAll(tempDir)

	certPaths, err := generateAllCertificates(tempDir)
	s.Require().NoError(err)

	options := &NATSOptions{
		URL:          "nats://localhost:4222",
		CABundleFile: certPaths.CAFile,
	}

	tlsConfig, err := getTLSConfig(options)
	s.Require().Error(err)
	s.Require().Nil(tlsConfig)
	s.Require().Contains(err.Error(), "incomplete mTLS configuration")
}

func (s *NATSSuite) TestGetTLSConfig_PartialConfig_CertAndKey() {
	tempDir, err := os.MkdirTemp("", "nats-mtls-test-*")
	s.Require().NoError(err)
	defer os.RemoveAll(tempDir)

	certPaths, err := generateAllCertificates(tempDir)
	s.Require().NoError(err)

	options := &NATSOptions{
		URL:                      "nats://localhost:4222",
		ClientCertificateFile:    certPaths.ClientCertFile,
		ClientCertificateKeyFile: certPaths.ClientKeyFile,
	}

	tlsConfig, err := getTLSConfig(options)
	s.Require().Error(err)
	s.Require().Nil(tlsConfig)
	s.Require().Contains(err.Error(), "incomplete mTLS configuration")
}

func (s *NATSSuite) TestConnectWithMTLS_Success() {
	tempDir, err := os.MkdirTemp("", "nats-mtls-test-*")
	s.Require().NoError(err)
	defer os.RemoveAll(tempDir)

	certPaths, err := generateAllCertificates(tempDir)
	s.Require().NoError(err)

	serverInfo, err := startMTLSNATSServerWithCerts(certPaths.ServerCertFile, certPaths.ServerKeyFile, certPaths.CAFile, s.timeout)
	s.Require().NoError(err)
	defer serverInfo.Server.Shutdown()
	defer serverInfo.Client.Close()

	addressIdentifiers := []AddressIdentifier{
		{Protocol: "ethereum", Network: "mainnet", Address: "0x1234567890123456789012345678901234567890"},
	}

	subscription, err := setupAddressListResponder(serverInfo.Client, addressIdentifiers, s.T().Logf)
	s.Require().NoError(err)
	defer subscription.Unsubscribe()

	s.resetPackageVariables()

	options := &NATSOptions{
		URL:                      serverInfo.URL,
		ClientCertificateFile:    certPaths.ClientCertFile,
		ClientCertificateKeyFile: certPaths.ClientKeyFile,
		CABundleFile:             certPaths.CAFile,
	}

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

	err = InitNATS(s.Suite.T().Context(), options, addressHandler, contractAddressHandler)
	s.Require().NoError(err)

	s.waitForCondition(func() bool {
		return isNATSConnected() && hasNATSSubscription()
	}, "should connect with mTLS")

	s.lock.Lock()
	defer s.lock.Unlock()
	s.Require().Len(s.addresses, 1)
	s.Require().Len(s.contractAddresses, 1)
}

func (s *NATSSuite) TestConnectWithMTLS_InvalidCertificate() {
	tempDir, err := os.MkdirTemp("", "nats-mtls-test-*")
	s.Require().NoError(err)
	defer os.RemoveAll(tempDir)

	certPaths, err := generateAllCertificates(tempDir)
	s.Require().NoError(err)

	serverInfo, err := startMTLSNATSServerWithCerts(certPaths.ServerCertFile, certPaths.ServerKeyFile, certPaths.CAFile, s.timeout)
	s.Require().NoError(err)
	defer serverInfo.Server.Shutdown()
	defer serverInfo.Client.Close()

	s.resetPackageVariables()

	options := &NATSOptions{
		URL:                      serverInfo.URL,
		ClientCertificateFile:    certPaths.InvalidCertFile,
		ClientCertificateKeyFile: certPaths.InvalidKeyFile,
		CABundleFile:             certPaths.CAFile,
	}

	err = InitNATS(
		s.Suite.T().Context(),
		options,
		func(_ common.Address) error { return nil },
		func(_ common.Address) error { return nil },
	)
	s.Require().Error(err)
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

// Standalone helper functions for mTLS tests

// CertificatePaths contains file paths for generated certificates
type CertificatePaths struct {
	CAFile          string
	ServerCertFile  string
	ServerKeyFile   string
	ClientCertFile  string
	ClientKeyFile   string
	InvalidCertFile string
	InvalidKeyFile  string
}

// NATSServerInfo contains information about a started NATS server
type NATSServerInfo struct {
	Server  *server.Server
	Options *server.Options
	URL     string
	Client  *nats.Conn
}

// generateAllCertificates generates all certificates needed for mTLS testing
func generateAllCertificates(tempDir string) (CertificatePaths, error) {
	// Generate CA
	caKey, caCert, err := generateCA()
	if err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to generate CA: %w", err)
	}

	caCertFile := filepath.Join(tempDir, "ca-cert.pem")
	if err := writeCert(tempDir, "ca-cert.pem", caCert); err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to write CA cert: %w", err)
	}

	// Generate server certificate
	serverKey, serverCert, err := generateCert(caKey, caCert, "server", []string{"localhost", "127.0.0.1"})
	if err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to generate server cert: %w", err)
	}

	serverCertFile := filepath.Join(tempDir, "server-cert.pem")
	serverKeyFile := filepath.Join(tempDir, "server-key.pem")
	if err := writeCert(tempDir, "server-cert.pem", serverCert); err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to write server cert: %w", err)
	}
	if err := writeKey(tempDir, "server-key.pem", serverKey); err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to write server key: %w", err)
	}

	// Generate client certificate
	clientKey, clientCert, err := generateCert(caKey, caCert, "client", nil)
	if err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to generate client cert: %w", err)
	}

	clientCertFile := filepath.Join(tempDir, "client-cert.pem")
	clientKeyFile := filepath.Join(tempDir, "client-key.pem")
	if err := writeCert(tempDir, "client-cert.pem", clientCert); err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to write client cert: %w", err)
	}
	if err := writeKey(tempDir, "client-key.pem", clientKey); err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to write client key: %w", err)
	}

	// Generate invalid certificate (self-signed, not signed by CA)
	invalidKey, invalidCert, err := generateCA()
	if err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to generate invalid cert: %w", err)
	}

	invalidCertFile := filepath.Join(tempDir, "invalid-cert.pem")
	invalidKeyFile := filepath.Join(tempDir, "invalid-key.pem")
	if err := writeCert(tempDir, "invalid-cert.pem", invalidCert); err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to write invalid cert: %w", err)
	}
	if err := writeKey(tempDir, "invalid-key.pem", invalidKey); err != nil {
		return CertificatePaths{}, fmt.Errorf("failed to write invalid key: %w", err)
	}

	return CertificatePaths{
		CAFile:          caCertFile,
		ServerCertFile:  serverCertFile,
		ServerKeyFile:   serverKeyFile,
		ClientCertFile:  clientCertFile,
		ClientKeyFile:   clientKeyFile,
		InvalidCertFile: invalidCertFile,
		InvalidKeyFile:  invalidKeyFile,
	}, nil
}

// generateCA generates a CA certificate and private key
func generateCA() (*ecdsa.PrivateKey, *x509.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
			CommonName:   "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse certificate: %w", err)
	}

	return key, cert, nil
}

// generateCert generates a certificate signed by the CA
func generateCert(caKey *ecdsa.PrivateKey, caCert *x509.Certificate, commonName string, dnsNames []string) (*ecdsa.PrivateKey, *x509.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
			CommonName:   commonName,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    dnsNames,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse certificate: %w", err)
	}

	return key, cert, nil
}

// writeCert writes a certificate to a PEM file
func writeCert(dir, filename string, cert *x509.Certificate) error {
	certFile := filepath.Join(dir, filename)
	certOut, err := os.Create(certFile)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}); err != nil {
		return fmt.Errorf("failed to encode PEM: %w", err)
	}

	return nil
}

// writeKey writes a private key to a PEM file
func writeKey(dir, filename string, key *ecdsa.PrivateKey) error {
	keyFile := filepath.Join(dir, filename)
	keyOut, err := os.Create(keyFile)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer keyOut.Close()

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("failed to marshal key: %w", err)
	}

	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		return fmt.Errorf("failed to encode PEM: %w", err)
	}

	return nil
}

// startMTLSNATSServerWithCerts starts a NATS server configured with mTLS
func startMTLSNATSServerWithCerts(serverCertFile, serverKeyFile, caCertFile string, timeout time.Duration) (NATSServerInfo, error) {
	// Create TLS config for server
	cert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	if err != nil {
		return NATSServerInfo{}, fmt.Errorf("failed to load key pair: %w", err)
	}

	caPool := x509.NewCertPool()
	caCertBytes, err := os.ReadFile(caCertFile)
	if err != nil {
		return NATSServerInfo{}, fmt.Errorf("failed to read CA file: %w", err)
	}
	caPool.AppendCertsFromPEM(caCertBytes)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS12,
	}

	natsServerOptions := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random port
		NoLog:     true,
		NoSigs:    true,
		TLSConfig: tlsConfig,
	}

	natsServer, err := server.NewServer(natsServerOptions)
	if err != nil {
		return NATSServerInfo{}, fmt.Errorf("failed to create NATS server: %w", err)
	}

	go natsServer.Start()

	if !natsServer.ReadyForConnections(timeout) {
		return NATSServerInfo{}, fmt.Errorf("NATS server not ready within timeout")
	}

	natsServerURL := "nats://" + natsServer.Addr().String()

	// Create mTLS client for mock responder
	clientTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
	}

	natsClient, err := nats.Connect(
		natsServerURL,
		nats.Secure(clientTLSConfig),
		nats.MaxReconnects(-1),
	)
	if err != nil {
		natsServer.Shutdown()
		return NATSServerInfo{}, fmt.Errorf("failed to connect NATS client: %w", err)
	}

	return NATSServerInfo{
		Server:  natsServer,
		Options: natsServerOptions,
		URL:     natsServerURL,
		Client:  natsClient,
	}, nil
}

// setupAddressListResponder sets up a NATS subscription to respond to address list requests
func setupAddressListResponder(natsClient *nats.Conn, addresses []AddressIdentifier, logger func(format string, args ...interface{})) (*nats.Subscription, error) {
	natsSubscription, err := natsClient.Subscribe(
		SubjectAddressListRequest,
		func(msg *nats.Msg) {
			response, err := json.Marshal(addresses)
			if err != nil {
				if logger != nil {
					logger("Failed to marshal: %v", err)
				}
				return
			}
			if err := msg.Respond(response); err != nil {
				if logger != nil {
					logger("Failed to respond: %v", err)
				}
			}
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}
	return natsSubscription, nil
}
