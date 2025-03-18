package actions

import (
	"context"
	"log/slog"
	"math/big"
	"time"

	"github.com/Zellic/EVM-trackooor/shared"
	"github.com/Zellic/EVM-trackooor/utils"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ethereum/go-ethereum/common"
)

var monitoredAddresses = mapset.NewSet[common.Address]()

func (p action) InitMonitorTransfers() {
	// monitor addresses for transactions
	for _, address := range p.o.Addresses {
		monitoredAddresses.Add(address)
		addTxAddressAction(address, handleAddressTx)
	}

	// monitor erc20 token for transfer events
	for _, addrInterface := range p.o.CustomOptions["erc20-tokens"].([]any) {
		erc20TokenAddress := common.HexToAddress(addrInterface.(string))
		addAddressEventSigAction(
			erc20TokenAddress,
			"Transfer(address,address,uint256)",
			handleTokenTransfer,
		)
	}

	// monitor addresses for transfer events
	addressChannel := make(chan common.Address)
	go shared.SubscribeToAddressEvents(context.Background(), addressChannel)
	go func() {
		for address := range addressChannel {
			monitoredAddresses.Add(address)
			addTxAddressAction(address, handleAddressTx)
		}
	}()

}

// called when a tx is from/to monitored address
func handleAddressTx(p ActionTxData) {
	var (
		from            = *p.From
		to              = *p.To
		value           = p.Transaction.Value()
		webhookData     = p.ConvertToWebhookTxData()
		isFromMonitored = monitoredAddresses.Contains(from)
		isToMonitored   = monitoredAddresses.Contains(to)
	)

	if value.Cmp(big.NewInt(0)) == 0 {
		return
	}

	// Publish to NATS
	if err := shared.PublishTransfer(webhookData); err != nil {
		slog.Error("Failed to publish webhook data", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if isFromMonitored {
		// Get sender's ETH balance
		balance, err := shared.Client.BalanceAt(ctx, from, nil) // nil means latest block
		if err != nil {
			slog.Error("Failed to get ETH balance",
				"wallet", from.String(),
				"error", err)
		} else {
			slog.Info("ETH balance after sending transfer",
				"wallet", from.String(),
				"balance", utils.FormatDecimals(balance, 18))

			// Convert to balance webhook format and publish
			balanceWebhook := p.ToSourceBalance(balance)
			if err := shared.PublishTransfer(balanceWebhook); err != nil {
				slog.Error("Failed to publish balance webhook data", "error", err)
			}
		}
	}

	if isToMonitored {
		// Get receiver's ETH balance
		balance, err := shared.Client.BalanceAt(ctx, to, nil) // nil means latest block
		if err != nil {
			slog.Error("Failed to get ETH balance",
				"wallet", to.String(),
				"error", err)
		} else {
			slog.Info("ETH balance after receiving transfer",
				"wallet", to.String(),
				"balance", utils.FormatDecimals(balance, 18))

			// Convert to balance webhook format and publish
			balanceWebhook := p.ToDestinationBalance(balance)
			if err := shared.PublishTransfer(balanceWebhook); err != nil {
				slog.Error("Failed to publish balance webhook data", "error", err)
			}
		}
	}
}

// called when erc20 token we're tracking emits Transfer event
func handleTokenTransfer(p ActionEventData) {
	var (
		from            = p.DecodedTopics["from"].(common.Address)
		to              = p.DecodedTopics["to"].(common.Address)
		value           = p.DecodedData["value"].(*big.Int)
		isFromMonitored = monitoredAddresses.Contains(from)
		isToMonitored   = monitoredAddresses.Contains(to)
		token           = p.EventLog.Address
		tokenInfo       = shared.RetrieveERC20Info(token)
		decimals        = tokenInfo.Decimals
		symbol          = tokenInfo.Symbol
	)

	if value.Cmp(big.NewInt(0)) == 0 {
		return
	}

	// Convert to transfer webhook format and publish
	webhookData := p.ConvertToWebhookLogData()
	if err := shared.PublishTransfer(webhookData); err != nil {
		slog.Error("Failed to publish webhook data", "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if isFromMonitored {
		// Get sender's token balance
		balance, err := shared.GetERC20Balance(ctx, token, from)
		if err != nil {
			slog.Error("Failed to get token balance",
				"token", token.String(),
				"wallet", from.String(),
				"error", err)
		} else {
			slog.Info("Token balance after transfer",
				"token", symbol,
				"wallet", from.String(),
				"balance", utils.FormatDecimals(balance, decimals))

			// Convert to balance webhook format and publish
			balanceWebhook := p.ToSourceBalance(balance)
			if err := shared.PublishTransfer(balanceWebhook); err != nil {
				slog.Error("Failed to publish balance webhook data", "error", err)
			}
		}
	}

	if isToMonitored {
		// Get receiver's token balance
		balance, err := shared.GetERC20Balance(ctx, token, to)
		if err != nil {
			slog.Error("Failed to get token balance",
				"token", token.String(),
				"wallet", to.String(),
				"error", err)
		} else {
			slog.Info("Token balance after transfer",
				"token", symbol,
				"wallet", to.String(),
				"balance", utils.FormatDecimals(balance, decimals))

			// Convert to balance webhook format and publish
			balanceWebhook := p.ToDestinationBalance(balance)
			if err := shared.PublishTransfer(balanceWebhook); err != nil {
				slog.Error("Failed to publish balance webhook data", "error", err)
			}
		}
	}
}
