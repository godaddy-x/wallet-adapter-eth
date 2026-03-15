package eth

import (
	"fmt"

	adapterconfig "github.com/godaddy-x/wallet-adapter/config"
	"github.com/godaddy-x/wallet-adapter/chain"
)

// NewAdapter 从 INI 内容创建并注册适配器，通过 LoadAssetsConfig 初始化配置与 RPC 客户端后返回。依赖 github.com/godaddy-x/wallet-adapter/config 与 chain。
func NewAdapter(iniContent, symbol, fullName string, decimals int32) (*EthAdapter, error) {
	kv, err := adapterconfig.KVFromINIContent(iniContent, symbol)
	if err != nil {
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
