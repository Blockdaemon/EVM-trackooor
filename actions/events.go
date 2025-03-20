package actions

import (
	"math/big"
	"time"

	"github.com/Zellic/EVM-trackooor/shared"
	"github.com/ethereum/go-ethereum/common"

	"go.blockdaemon.com/aegis-event-streaming/pkg/servers/webhook"
)

const (
	AssetNative             = "native"
	ChainID                 = "eip155:11155111"
	EventTypeTransaction    = "unified_confirmed_tx"
	EventTypeTransactionLog = "unified_confirmed_tx_log"
	EventTypeBalance        = "unified_confirmed_balance"
	Network                 = "sepolia"
	ProtocolEthereum        = "ethereum"
	StatusSuccess           = "success"
)

// ToSourceBalance converts ActionTxData to WebhookMessageUnifiedConfirmedBalanceRequest
// with the latest ETH balance
func (txData *ActionTxData) ToSourceBalance(balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	return txData.toBalance(txData.From, balance)
}

func (txData *ActionTxData) ToDestinationBalance(balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	return txData.toBalance(txData.To, balance)
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
		ChainId:   ChainID,
		Data:      balanceData,
		EventType: EventTypeBalance,
		Network:   Network,
		Protocol:  ProtocolEthereum,
	}
}

// ConvertToWebhookTxData converts ActionTxData to WebhookMessageUnifiedConfirmedTxRequest
func (txData *ActionTxData) ConvertToWebhookTxData() webhook.WebhookMessageUnifiedConfirmedTxRequest {
	// Create tx hash string
	txHash := txData.Transaction.Hash().String()

	// Create transfer data for native ETH transfer
	var transfers []webhook.Transfer
	if txData.Transaction.Value().Sign() > 0 && txData.From != nil && txData.To != nil {
		fromStr := txData.From.String()
		toStr := txData.To.String()
		assetStr := AssetNative

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
	// FIXME: How to set this?
	print(new(big.Int).Mul(new(big.Int).SetUint64(gasLimit), gasPrice))

	// // Create fee data
	// feeJson := struct {
	// 	Amount   string `json:"amount"`
	// 	Currency string `json:"currency"`
	// }{
	// 	Amount:   fee.String(),
	// 	Currency: "ETH",
	// }
	// feeData := webhook.WebhookMessageUnifiedConfirmedTxData_Fee{
	// 	union: feeJson,
	// }

	return webhook.WebhookMessageUnifiedConfirmedTxRequest{
		ChainId: ChainID,
		Data: webhook.WebhookMessageUnifiedConfirmedTxData{
			BlockHash:   txData.Block.Hash().String(),
			BlockNumber: txData.Block.Number().Uint64(),
			Fee:         nil,
			Status:      StatusSuccess,
			Timestamp:   uint64(txData.Block.Time()),
			Transfers:   transfers,
			TxHash:      &txHash,
			TxId:        txHash,
		},
		EventType: EventTypeTransaction,
		Network:   Network,
		Protocol:  ProtocolEthereum,
	}
}

// ConvertToWebhookLogData converts ActionEventData to WebhookMessageUnifiedConfirmedTxLogData
func (eventData *ActionEventData) ConvertToWebhookLogData() webhook.WebhookMessageUnifiedConfirmedTxLogRequest {
	// Create tx hash string
	txHash := eventData.EventLog.TxHash.String()

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
		ChainId: ChainID,
		Data: webhook.WebhookMessageUnifiedConfirmedTxLogData{
			BlockHash:   eventData.EventLog.BlockHash.String(),
			BlockNumber: eventData.EventLog.BlockNumber,
			Status:      StatusSuccess,
			Timestamp:   uint64(time.Now().Unix()), // Current time as we don't have block time in log
			Transfers:   transfers,
			TxHash:      &txHash,
			TxId:        txHash,
		},
		EventType: EventTypeTransactionLog,
		Network:   Network,
		Protocol:  ProtocolEthereum,
	}
}

// ToSourceBalance converts ActionEventData to WebhookMessageUnifiedConfirmedBalanceRequest
// with the latest ERC20 token balance
func (eventData *ActionEventData) ToSourceBalance(balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	// Get address from the event (using 'from' address)
	address := eventData.DecodedTopics["from"].(common.Address)

	return eventData.toBalance(&address, balance)
}

// ToDestinationBalance converts ActionEventData to WebhookMessageUnifiedConfirmedBalanceRequest
// with the latest ERC20 token balance
func (eventData *ActionEventData) ToDestinationBalance(balance *big.Int) webhook.WebhookMessageUnifiedConfirmedBalanceRequest {
	// Get address from the event (using 'to' address)
	address := eventData.DecodedTopics["to"].(common.Address)

	return eventData.toBalance(&address, balance)
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
		ChainId:   ChainID,
		Data:      balanceData,
		EventType: EventTypeBalance,
		Network:   Network,
		Protocol:  ProtocolEthereum,
	}
}
