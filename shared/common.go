package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// GLOBAL VARIABLES

var Client *ethclient.Client
var ChainID *big.Int

// command args (to be set by main.go)
var RpcURL string
var Verbose bool
var Options TrackooorOptions

// When enabled, event subscriptions will use dual topic filters to monitor any
// ERC20 transfers where either the sender (topic1) or receiver (topic2)
// matches one of the registered wallet addresses. This allows server-side
// filtering by the node without specifying token contract addresses.
var UseDualTransferWalletFilters bool

// Pre-encoded wallet addresses as 32-byte topic hashes for filtering
// (left-padded address bytes). Populated at runtime based on configured
// monitored wallet addresses.
var monitoredAddressHashes = mapset.NewSet[common.Hash]()

// Channel to notify when a new wallet address is added for server-side
// wallet-topic ERC20 Transfer subscriptions.
var NewWalletTopicChan chan common.Address

// JSON data
var EventSigs map[string]interface{} // from data file, maps hex string to event abi
var FuncSigs map[string]interface{}  // from data file, maps hex string (func selector) to func abi
var Blockscanners map[string]interface{}

// ERC20 data
var ERC20TokenInfos map[common.Address]ERC20Info

// cached data
var AddressTypeCacheMutex sync.RWMutex
var AddressTypeCache map[common.Address]int

// wait groups
var BlockWaitGroup sync.WaitGroup

// this is to auto determine whether to track blocks or events
var BlockTrackingRequired bool

// STRUCTS

// config options for trackooors
type TrackooorOptions struct {
	RpcURL            string
	FilterAddresses   []common.Address // global list of addresses to filter for
	FilterEventTopics [][]common.Hash  // global list of event topics to filter for

	AddressProperties map[common.Address]map[string]interface{} // e.g. map address to "name":"USDC"

	Actions map[string]ActionOptions // map action name to its options

	AutoFetchABI         bool
	MaxRequestsPerSecond int

	ListenerOptions   ListenerOptions
	HistoricalOptions HistoricalOptions

	IsL2Chain bool // will account for invalid tx types

	NATS NATSOptions
}

type ActionOptions struct {
	Addresses     []common.Address       // addresses specific to each action
	EventSigs     []common.Hash          // event sigs specific to each action
	CustomOptions map[string]interface{} // custom options specific to each action
}

type ListenerOptions struct {
	ListenToDeployments bool
}

type HistoricalOptions struct {
	FromBlock          *big.Int
	ToBlock            *big.Int
	StepBlocks         *big.Int
	ContinueToRealtime bool
	LoopBackwards      bool

	BatchFetchBlocks bool
}

type NATSOptions struct {
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"password"`
}

// already processed blocks, txs or events
type ProcessedEntity struct {
	BlockNumber *big.Int
	TxHash      common.Hash
	EventIndex  uint
}

var AlreadyProcessed map[common.Hash]ProcessedEntity
var DupeDetected bool
var SwitchingToRealtime bool
var AreActionsInitialized bool

// constants
var ZeroAddress = common.HexToAddress("0x0000000000000000000000000000000000000000")

// info for each event from the json
type EventInfo struct {
	Name string
	Sig  string
	Abi  map[string]interface{}
}

// info of erc20 tokens
type ERC20Info struct {
	Name     string
	Symbol   string
	Decimals uint8
	Address  common.Address
}

// FUNCTIONS

func init() {
	// init maps
	ERC20TokenInfos = make(map[common.Address]ERC20Info)
	AddressTypeCache = make(map[common.Address]int)
	NewWalletTopicChan = make(chan common.Address, 1024)
}

// MonitoredAddressHashes returns the thread-safe set of monitored address hashes.
func MonitoredAddressHashes() mapset.Set[common.Hash] {
	return monitoredAddressHashes
}

func Infof(logger *slog.Logger, format string, args ...any) {
	logger.Info(fmt.Sprintf(format, args...))
}

func Warnf(logger *slog.Logger, format string, args ...any) {
	logger.Warn(fmt.Sprintf(format, args...))
}

func TimeLogger(whichController string) *log.Logger {
	return log.New(os.Stderr, whichController, log.Lmsgprefix|log.LstdFlags)
}

func ConnectToRPC(rpcURL string) (*ethclient.Client, *big.Int) {
	Infof(slog.Default(), "Connecting to RPC URL...\n")
	// Build dial options and include optional HTTP/WebSocket headers from env
	var clientOptions []rpc.ClientOption
	clientOptions = append(clientOptions, rpc.WithWebsocketMessageSizeLimit(0))

	// Support explicit key/value envs first, fallback to legacy RPC_HEADER="Key:Value"
	{
		var hdr http.Header
		if key, val := os.Getenv("RPC_HEADER_KEY"), os.Getenv("RPC_HEADER_VALUE"); key != "" && val != "" {
			hdr = http.Header{}
			hdr.Add(strings.TrimSpace(key), strings.TrimSpace(val))
		}
		if hdr != nil {
			clientOptions = append(clientOptions, rpc.WithHeaders(hdr))
			for k := range hdr {
				Infof(slog.Default(), "Applied RPC header '%s' from environment\n", k)
			}
		}
	}

	RpcClient, err := rpc.DialOptions(
		context.Background(),
		rpcURL,
		clientOptions...,
	)
	client := ethclient.NewClient(RpcClient)

	if err != nil {
		log.Fatal(err)
	}
	Infof(slog.Default(), "Connected\n")
	Infof(slog.Default(), "Fetching Chain ID...\n")
	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	Infof(slog.Default(), "Chain ID: %d\n", chainID)
	return client, chainID
}

func LoadFromJson(filename string, v any) {
	Infof(slog.Default(), "Loading JSON file %v", filename)
	f, _ := os.ReadFile(filename)
	err := json.Unmarshal(f, v)
	if err != nil {
		log.Fatalf("LoadFromJson error: %v\n", err)
	}
}
