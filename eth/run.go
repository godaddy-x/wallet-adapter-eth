package eth

import (
	"fmt"

	"github.com/blockchain/wallet-adapter-eth/internal/config"
	"github.com/blockchain/wallet-adapter/chain"
)

// NewAdapter 从 INI 内容文本创建并注册适配器，设置 RPC 客户端后返回。
// iniContent 为完整 INI 文本，symbol 为段名与链标识（如 "ETH"），fullName 为链全称，decimals 为精度（通常 18）。
func NewAdapter(iniContent, symbol, fullName string, decimals int32) (*EthAdapter, error) {
	cfg, client, err := config.LoadFromINIContent(iniContent, symbol)
	if err != nil {
		return nil, err
	}

	adapter, err := chain.GetAdapter(symbol)
	if err != nil {
		adapter = NewEthAdapter(cfg, symbol, fullName, decimals)
		chain.RegAdapter(symbol, adapter)
	}

	ethAdapter, ok := adapter.(*EthAdapter)
	if !ok {
		return nil, fmt.Errorf("eth adapter fail")
	}

	ethAdapter.SetClient(client)

	if ethAdapter.Config().ChainID == 0 {
		_, err = ethAdapter.SetNetworkChainID()
		if err != nil {
			return nil, fmt.Errorf("eth adapter SetNetworkChainID error: %w", err)
		}
	}
	return ethAdapter, nil
}
