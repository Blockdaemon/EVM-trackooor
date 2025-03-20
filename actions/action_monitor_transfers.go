package actions

import (
	"context"
	"log/slog"
	"math/big"
	"os"
	"time"

	"github.com/Zellic/EVM-trackooor/shared"
	"github.com/Zellic/EVM-trackooor/utils"
	"github.com/ethereum/go-ethereum/common"
)

func (p action) InitMonitorTransfers() {
	shared.SetAddressAddedCallback(
		func(addr common.Address) {
			addTxAddressAction(addr, handleAddressTx)
		},
	)

	// monitor addresses for transactions
	for _, address := range p.o.Addresses {
		if err := shared.AddMonitoredAddress(address); err != nil {
			slog.Error("failed to add monitored address", "address", address, "error", err)
			os.Exit(1) // TODO: Bad!
		}
	}

	// monitor erc20 token for transfer events
	erc20TokenAddresses := p.o.CustomOptions["erc20-tokens"].([]any)
	for _, erc20TokenAddress := range erc20TokenAddresses {
		erc20TokenAddressString := erc20TokenAddress.(string)

		addAddressEventSigAction(
			common.HexToAddress(erc20TokenAddressString),
			"Transfer(address,address,uint256)",
			handleTokenTransfer,
		)
	}
}

// called when a tx is from/to monitored address
func handleAddressTx(p ActionTxData) {
	var (
		from            = *p.From
		to              = *p.To
		value           = p.Transaction.Value()
		webhookData     = p.ConvertToWebhookTxData()
		isFromMonitored = shared.MonitoredAddressesContain(from)
		isToMonitored   = shared.MonitoredAddressesContain(to)
	)

	if !isFromMonitored && !isToMonitored {
		return
	}

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
		token           = p.EventLog.Address
		isFromMonitored = shared.MonitoredAddressesContain(from)
		isToMonitored   = shared.MonitoredAddressesContain(to)
	)

	if !isFromMonitored && !isToMonitored {
		return
	}

	if value.Cmp(big.NewInt(0)) == 0 {
		return
	}

	var (
		tokenInfo = shared.RetrieveERC20Info(token)
		decimals  = tokenInfo.Decimals
		symbol    = tokenInfo.Symbol
	)

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
