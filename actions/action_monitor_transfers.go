package actions

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/Zellic/EVM-trackooor/shared"
	"github.com/Zellic/EVM-trackooor/utils"
	"github.com/ethereum/go-ethereum/common"
)

const (
	eventSignatureTransfer = "Transfer(address,address,uint256)"
)

var (
	monitoredAddresses = mapset.NewSet[common.Address]()
)

// addressMatchesWalletTopics returns true if the given address matches any of
// the wallet-topic filters (used when server-side topic filtering is enabled).
func addressMatchesWalletTopics(addr common.Address) bool {
	shared.FilterWalletTopicsMutex.RLock()
	defer shared.FilterWalletTopicsMutex.RUnlock()

	if len(shared.FilterWalletTopics) == 0 {
		return false
	}
	hashed := common.BytesToHash(addr.Bytes())
	for _, h := range shared.FilterWalletTopics {
		if h == hashed {
			return true
		}
	}
	return false
}

func AddAddressToMonitoredAddresses(address common.Address) error {
	addTxAddressAction(address, handleAddressTx)
	monitoredAddresses.Add(address)

	// Notify the event listener to create wallet-topic ERC20 Transfer
	// subscriptions for this new address (server-side filtering) and update
	// filters immediately for blocks/historical.
	shared.FilterWalletTopicsMutex.Lock()
	shared.FilterWalletTopics = append(shared.FilterWalletTopics, common.BytesToHash(address.Bytes()))
	shared.FilterWalletTopicsMutex.Unlock()

	// Always print to console when a wallet address is added/updated
	fmt.Printf("Monitored wallet address added: %s\n", address.Hex())
	return nil
}

func (p action) InitMonitorTransfers() {
	// Always enable wallet-topic ERC20 Transfer filtering (from/to)
	shared.UseDualTransferWalletFilters = true

	// Addresses are dynamically added via NATS using AddAddressToMonitoredAddresses()
	// No need to read from config "addresses": {} field

	// Monitor all ERC20 Transfer events globally; filter by monitored addresses in handler
	addEventSigAction(eventSignatureTransfer, handleTokenTransfer)
}

// called when a tx is from/to monitored address
func handleAddressTx(tx ActionTxData) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if tx.To == nil { // contract creation
		receipt, err := shared.Client.TransactionReceipt(ctx, tx.Transaction.Hash())
		if err != nil {
			slog.Error("Failed to get transaction receipt", "hash", tx.Transaction.Hash(), "error", err)
			return
		}

		tx.To = &receipt.ContractAddress
	}

	var (
		from            = *tx.From
		to              = *tx.To
		webhookData     = tx.ToTransaction()
		isFromMonitored = monitoredAddresses.Contains(from)
		isToMonitored   = monitoredAddresses.Contains(to)
	)

	// Publish to NATS
	if err := shared.PublishSerializable(webhookData); err != nil {
		slog.Error("Failed to publish webhook data", "error", err)
		return
	}

	if isFromMonitored {
		// Get sender's ETH balance
		balance, err := shared.Client.BalanceAt(ctx, from, nil) // nil means latest block
		if err != nil {
			slog.Error(
				"Failed to get ETH balance",
				"from", from.String(),
				"error", err,
			)

			return
		}

		slog.Info(
			"ETH balance after sending transfer",
			"from", from.String(),
			"balance", utils.FormatDecimals(balance, 18),
		)

		// Convert to balance webhook format and publish
		balanceWebhook := tx.ToSourceBalance(balance)
		if err := shared.PublishSerializable(balanceWebhook); err != nil {
			slog.Error("Failed to publish balance webhook data", "error", err)

			return
		}
	}

	if isToMonitored {
		// Get receiver's ETH balance
		balance, err := shared.Client.BalanceAt(ctx, to, nil) // nil means latest block
		if err != nil {
			slog.Error(
				"Failed to get ETH balance",
				"to", to.String(),
				"error", err,
			)

			return
		}

		slog.Info(
			"ETH balance after receiving transfer",
			"to", to.String(),
			"balance", utils.FormatDecimals(balance, 18),
		)

		// Convert to balance webhook format and publish
		balanceWebhook := tx.ToDestinationBalance(balance)
		if err := shared.PublishSerializable(balanceWebhook); err != nil {
			slog.Error("Failed to publish balance webhook data", "error", err)

			return
		}
	}
}

// called when erc20 token we're tracking emits Transfer event
func handleTokenTransfer(event ActionEventData) {
	// Defensive guards: fields may be missing or of unexpected types; skip if so
	valueInterface, ok := event.DecodedData["value"]
	if !ok || valueInterface == nil {
		return
	}
	value, ok := valueInterface.(*big.Int)
	if !ok || value == nil {
		return
	}
	if value.Cmp(big.NewInt(0)) == 0 {
		return
	}

	fromInterface, ok := event.DecodedTopics["from"]
	if !ok || fromInterface == nil {
		return
	}
	toInterface, ok := event.DecodedTopics["to"]
	if !ok || toInterface == nil {
		return
	}
	from, ok := fromInterface.(common.Address)
	if !ok {
		return
	}
	to, ok := toInterface.(common.Address)
	if !ok {
		return
	}

	var (
		isFromMonitored = monitoredAddresses.Contains(from)
		isToMonitored   = monitoredAddresses.Contains(to)
	)

	// When using server-side wallet topic filtering, also treat matches to the
	// configured wallet topics as monitored (even if monitoredAddresses hasn't
	// been populated yet by NATS/config at this moment in time).
	if shared.UseDualTransferWalletFilters {
		if !(isFromMonitored || isToMonitored || addressMatchesWalletTopics(from) || addressMatchesWalletTopics(to)) {
			return
		}
	} else {
		if !isFromMonitored && !isToMonitored {
			return
		}
	}

	var (
		token     = event.EventLog.Address
		tokenInfo = shared.RetrieveERC20Info(token)
		decimals  = tokenInfo.Decimals
		symbol    = tokenInfo.Symbol
	)

	// Convert to transfer webhook format and publish
	webhookData := event.ToTransactionLog()
	if err := shared.PublishSerializable(webhookData); err != nil {
		slog.Error("Failed to publish webhook data", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if isFromMonitored {
		// Get sender's token balance
		balance, err := shared.GetERC20Balance(ctx, token, from)
		if err != nil {
			slog.Error(
				"Failed to get token balance",
				"token", token.String(),
				"wallet", from.String(),
				"error", err,
			)

			return
		}

		slog.Info("Token balance after transfer",
			"token", symbol,
			"wallet", from.String(),
			"balance", utils.FormatDecimals(balance, decimals))

		// Convert to balance webhook format and publish
		balanceWebhook := event.ToSourceBalance(balance)
		if err := shared.PublishSerializable(balanceWebhook); err != nil {
			slog.Error("Failed to publish balance webhook data", "error", err)

			return
		}
	}

	if isToMonitored {
		// Get receiver's token balance
		balance, err := shared.GetERC20Balance(ctx, token, to)
		if err != nil {
			slog.Error(
				"Failed to get token balance",
				"token", token.String(),
				"wallet", to.String(),
				"error", err,
			)

			return
		}

		slog.Info("Token balance after transfer",
			"token", symbol,
			"wallet", to.String(),
			"balance", utils.FormatDecimals(balance, decimals))

		// Convert to balance webhook format and publish
		balanceWebhook := event.ToDestinationBalance(balance)
		if err := shared.PublishSerializable(balanceWebhook); err != nil {
			slog.Error("Failed to publish balance webhook data", "error", err)

			return
		}
	}
}
