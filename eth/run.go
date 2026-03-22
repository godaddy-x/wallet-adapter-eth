package eth

import (
	"encoding/json"
	"fmt"

	"github.com/godaddy-x/wallet-adapter/chain"
	adapterconfig "github.com/godaddy-x/wallet-adapter/config"
)

// NewAdapter 从 JSON 字符串创建并注册适配器，通过 LoadAssetsConfig 初始化配置与 RPC 客户端后返回。
// 该接口使用 github.com/godaddy-x/wallet-adapter/config 的 MapConfig 解析 JSON 配置。
// jsonContent: JSON 格式的配置字符串（包含 serverAPI、broadcastAPI、dataDir 等字段）
// symbol: 链标识，如 ETH、BSC
// fullName: 链全称，如 Ethereum、Binance Smart Chain
// decimals: 链原生币精度，如 18
func NewAdapter(jsonContent, symbol, fullName string, decimals int32) (*EthAdapter, error) {
	kv := adapterconfig.MapConfig{}
	if err := json.Unmarshal([]byte(jsonContent), &kv); err != nil {
		return nil, err
	}

	adapter, err := chain.GetAdapter(symbol)
	if err != nil {
		adapter = NewEthAdapter(symbol, fullName, decimals)
		chain.RegAdapter(symbol, adapter)
	}

	ethAdapter, ok := adapter.(*EthAdapter)
	if !ok {
		return nil, fmt.Errorf("eth adapter fail")
	}

	if err = ethAdapter.LoadAssetsConfig(adapterconfig.MapConfig(kv)); err != nil {
		return nil, fmt.Errorf("eth adapter LoadAssetsConfig error: %w", err)
	}
	return ethAdapter, nil
}
