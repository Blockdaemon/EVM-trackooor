package actions

import (
	"context"
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

func AddAddressToMonitoredAddresses(address common.Address) error {
	addTxAddressAction(address, handleAddressTx)
	monitoredAddresses.Add(address)
	return nil
}

func AddContractAddressToMonitoredAddresses(contractAddress common.Address) error {
	addAddressEventSigAction(
		contractAddress,
		eventSignatureTransfer,
		handleTokenTransfer,
	)
	monitoredAddresses.Add(contractAddress)
	return nil
}

func (p action) InitMonitorTransfers() {
	// monitor addresses for transactions
	for _, address := range p.o.Addresses {
		AddAddressToMonitoredAddresses(address)
	}

	// monitor erc20 token for transfer events
	erc20TokenAddresses := p.o.CustomOptions["erc20-tokens"].([]any)
	for _, erc20TokenAddress := range erc20TokenAddresses {
		var (
			erc20TokenAddressString = erc20TokenAddress.(string)
			typedERC20TokenAddress  = common.HexToAddress(erc20TokenAddressString)
		)

		AddContractAddressToMonitoredAddresses(typedERC20TokenAddress)
	}
}

// called when a tx is from/to monitored address
func handleAddressTx(tx ActionTxData) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// fetch receipt to determine status and (if needed) deployed contract address
	receipt, err := shared.Client.TransactionReceipt(ctx, tx.Transaction.Hash())
	if err != nil {
		slog.Error("Failed to get transaction receipt", "hash", tx.Transaction.Hash(), "error", err)
		return
	}
	if receipt.Status == 1 {
		tx.Status = StatusSuccess
	} else {
		tx.Status = StatusFailure
	}
	// set deployed contract address if contract creation
	if tx.To == nil {
		tx.To = &receipt.ContractAddress
	}

	var (
		from            = *tx.From
		to              = *tx.To
		isFromMonitored = monitoredAddresses.Contains(from)
		isToMonitored   = monitoredAddresses.Contains(to)
	)

	// Convert to transfer webhook format and publish
	webhookData := tx.ToTransaction()
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
	value := event.DecodedData["value"].(*big.Int)
	if value.Cmp(big.NewInt(0)) == 0 {
		return
	}

	var (
		from            = event.DecodedTopics["from"].(common.Address)
		to              = event.DecodedTopics["to"].(common.Address)
		isFromMonitored = monitoredAddresses.Contains(from)
		isToMonitored   = monitoredAddresses.Contains(to)
	)

	if !isFromMonitored && !isToMonitored {
		return
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
