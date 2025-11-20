package actions

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/Zellic/EVM-trackooor/shared"
	"github.com/ethereum/go-ethereum/common"

	"go.blockdaemon.com/aegis-event-streaming/pkg/servers/webhook"
)

const (
	AssetNative             = "native"
	EventTypeTransaction    = "unified_confirmed_tx"
	EventTypeTransactionLog = "unified_confirmed_tx_log"
	EventTypeBalance        = "unified_confirmed_balance"
	ProtocolEthereum        = "ethereum"
	StatusSuccess           = "success"
	StatusFailure           = "failed"
)

var networkMap = map[string]string{
	"1":        "mainnet",
	"5":        "goerli",
	"2026":     "lower-qa",
	"2025":     "higher-prod",
	"17069":    "holesky",
	"11155111": "sepolia",
}

func (txData *ActionTxData) ToDestinationBalance(balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	return txData.toBalance(txData.To, balance)
}

// ToSourceBalance converts ActionTxData to WebhookMessageUnifiedConfirmedBalanceRequest
// with the latest ETH balance
func (txData *ActionTxData) ToSourceBalance(balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	return txData.toBalance(txData.From, balance)
}

// ToTransaction converts ActionTxData to WebhookMessageUnifiedConfirmedTxRequest
func (txData *ActionTxData) ToTransaction() webhook.WebhookMessageUnifiedConfirmedTxRequest {
	// Create tx hash string
	txHash := txData.Transaction.Hash().String()

	// derive status from txData if set, otherwise default to success
	status := txData.Status
	if status == "" {
		status = StatusSuccess
	}

	// Create transfer data for native ETH transfer
	var transfers []webhook.Transfer
	if txData.From != nil { // the to address might be nil (contract deployment); the value might be 0 (contract call)
		var (
			fromStr  = txData.From.String()
			toStr    = txData.To.String()
			assetStr = AssetNative
		)

		transfers = append(
			transfers,
			webhook.Transfer{
				From:  &fromStr,
				To:    &toStr,
				Asset: &assetStr,
				// EventName: , // not required
				Value: txData.Transaction.Value(),
			},
		)
	}

	// Calculate basic fee (without receipt, we can only estimate based on gas limit)
	gasLimit := txData.Transaction.Gas()
	gasPrice := txData.Transaction.GasPrice()
	if txData.Transaction.Type() == 2 { // EIP-1559
		if txData.Block.BaseFee() != nil && txData.Transaction.GasTipCap() != nil {
			gasPrice = new(big.Int).Add(
				txData.Block.BaseFee(),
				txData.Transaction.GasTipCap(),
			)
		}
	}

	// FIXME: How to set fee?
	print(new(big.Int).Mul(new(big.Int).SetUint64(gasLimit), gasPrice))

	return webhook.WebhookMessageUnifiedConfirmedTxRequest{
		ChainId: prefixedChainIDString(shared.ChainID),
		Data: webhook.WebhookMessageUnifiedConfirmedTxData{
			BlockHash:   txData.Block.Hash().String(),
			BlockNumber: txData.Block.Number().Uint64(),
			Fee:         nil,
			Status:      status,
			Timestamp:   uint64(txData.Block.Time()),
			Transfers:   transfers,
			TxHash:      &txHash,
			TxId:        txHash,
		},
		EventType: EventTypeTransaction,
		Network:   networkName(shared.ChainID),
		Protocol:  ProtocolEthereum,
	}
}

func (txData *ActionTxData) toBalance(address *common.Address, balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	balanceData := webhook.WebhookMessageUnifiedConfirmedBalanceData{
		Address:        address.String(),
		Asset:          AssetNative,
		BlockHash:      txData.Block.Hash().String(),
		BlockNumber:    txData.Block.Number().Uint64(),
		BlockTimestamp: uint64(txData.Block.Time()),
		Value:          balance,
	}

	return webhook.WebhookMessageUnifiedConfirmedBalanceRequest{
		ChainId:   prefixedChainIDString(shared.ChainID),
		Data:      balanceData,
		EventType: EventTypeBalance,
		Network:   networkName(shared.ChainID),
		Protocol:  ProtocolEthereum,
	}
}

// ToDestinationBalance converts ActionEventData to WebhookMessageUnifiedConfirmedBalanceRequest
// with the latest ERC20 token balance
func (eventData *ActionEventData) ToDestinationBalance(balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	// Get address from the event (using 'to' address)
	address := eventData.DecodedTopics["to"].(common.Address)

	return eventData.toBalance(&address, balance)
}

// ToSourceBalance converts ActionEventData to WebhookMessageUnifiedConfirmedBalanceRequest
// with the latest ERC20 token balance
func (eventData *ActionEventData) ToSourceBalance(balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	// Get address from the event (using 'from' address)
	address := eventData.DecodedTopics["from"].(common.Address)

	return eventData.toBalance(&address, balance)
}

// ToTransactionLog converts ActionEventData to WebhookMessageUnifiedConfirmedTxLogData
func (eventData *ActionEventData) ToTransactionLog() webhook.WebhookMessageUnifiedConfirmedTxLogRequest {
	// Create tx hash string
	txHash := eventData.EventLog.TxHash.String()

	// derive status from receipt if available (logs usually imply success, but be explicit)
	status := StatusSuccess
	if receipt, err := shared.Client.TransactionReceipt(context.Background(), eventData.EventLog.TxHash); err == nil {
		if receipt.Status == 1 {
			status = StatusSuccess
		} else {
			status = StatusFailure
		}
	}

	// Create transfer data from the event
	var transfers []webhook.Transfer

	// For ERC20 Transfer events
	fromStr := eventData.DecodedTopics["from"].(common.Address).String()
	toStr := eventData.DecodedTopics["to"].(common.Address).String()
	value := eventData.DecodedData["value"].(*big.Int)

	// Get token info
	token := eventData.EventLog.Address
	tokenInfo := shared.RetrieveERC20Info(token)
	assetStr := tokenInfo.Address.String()

	transfers = append(
		transfers,
		webhook.Transfer{
			From:  &fromStr,
			To:    &toStr,
			Asset: &assetStr,
			// EventName: , // not required
			Value: value,
		},
	)

	return webhook.WebhookMessageUnifiedConfirmedTxLogRequest{
		ChainId: prefixedChainIDString(shared.ChainID),
		Data: webhook.WebhookMessageUnifiedConfirmedTxLogData{
			BlockHash:   eventData.EventLog.BlockHash.String(),
			BlockNumber: eventData.EventLog.BlockNumber,
			Status:      status,
			Timestamp:   uint64(time.Now().Unix()), // Current time as we don't have block time in log
			Transfers:   transfers,
			TxHash:      &txHash,
			TxId:        txHash,
		},
		EventType: EventTypeTransactionLog,
		Network:   networkName(shared.ChainID),
		Protocol:  ProtocolEthereum,
	}
}

func (eventData *ActionEventData) toBalance(address *common.Address, balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	var (
		contractAddress = eventData.EventLog.Address
		balanceData     = webhook.WebhookMessageUnifiedConfirmedBalanceData{
			Address:        address.String(),
			Asset:          contractAddress.String(),
			BlockHash:      eventData.EventLog.BlockHash.String(),
			BlockNumber:    eventData.EventLog.BlockNumber,
			BlockTimestamp: uint64(time.Now().Unix()),
			Value:          balance,
		}
	)

	return webhook.WebhookMessageUnifiedConfirmedBalanceRequest{
		ChainId:   prefixedChainIDString(shared.ChainID),
		Data:      balanceData,
		EventType: EventTypeBalance,
		Network:   networkName(shared.ChainID),
		Protocol:  ProtocolEthereum,
	}
}

func networkName(chainID *big.Int) string {
	name, ok := networkMap[chainID.String()]
	if !ok {
		return ""
	}
	return name
}

func prefixedChainIDString(chainID *big.Int) string {
	return fmt.Sprintf("eip155:%d", chainID)
}
