package shared

import (
	"context"
	"fmt"
	"math/big"

	"github.com/Zellic/EVM-trackooor/contracts/IERC20Metadata"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
)

// GetERC20Balance returns the current balance of an ERC20 token for a given address
func GetERC20Balance(ctx context.Context, token, address common.Address) (*big.Int, error) {
	tokenInstance, err := IERC20Metadata.NewIERC20Metadata(token, Client)
	if err != nil {
		return nil, fmt.Errorf("failed to create ERC20 metadata instance: %w", err)
	}

	balance, err := tokenInstance.BalanceOf(&bind.CallOpts{Context: ctx}, address)
	if err != nil {
		return nil, fmt.Errorf("failed to get balance for token %s and address %s: %w",
			token.Hex(), address.Hex(), err)
	}
	return balance, nil
}
