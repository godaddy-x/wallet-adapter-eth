package eth

import (
	"math/big"

	"github.com/blockchain/wallet-adapter-eth/internal/config"
	decoder2 "github.com/blockchain/wallet-adapter-eth/internal/decoder"
	"github.com/blockchain/wallet-adapter-eth/internal/manager"
	"github.com/blockchain/wallet-adapter-eth/internal/rpc"
	"github.com/blockchain/wallet-adapter/chain"
	"github.com/blockchain/wallet-adapter/decoder"
	"github.com/blockchain/wallet-adapter/scanner"
	"github.com/blockchain/wallet-adapter/types"
)

// EthAdapter 实现 wallet-adapter 的 ChainAdapter，用于 ETH 及 EVM 兼容链（BSC、Polygon、Arbitrum 等）
type EthAdapter struct {
	chain.ChainAdapterBase
	wm       *manager.WalletManager
	txDec    *decoder2.EthTransactionDecoder
	addrDec  *decoder2.EthAddressDecoder
	symbol   string
	fullName string
	decimals int32
}

// NewEthAdapter 创建以太坊链适配器（symbol 如 ETH、BSC、MATIC、ARB）
func NewEthAdapter(cfg *config.WalletConfig, symbol, fullName string, decimals int32) *EthAdapter {
	wm := &manager.WalletManager{Config: cfg}
	txDec := decoder2.NewTransactionDecoder(wm)
	addrDec := decoder2.DefaultDecoder
	return &EthAdapter{
		wm:       wm,
		txDec:    txDec,
		addrDec:  addrDec,
		symbol:   symbol,
		fullName: fullName,
		decimals: decimals,
	}
}

// Symbol 币种标识
func (a *EthAdapter) Symbol() string {
	if a.symbol != "" {
		return a.symbol
	}
	return a.wm.Config.Symbol
}

// FullName 全称
func (a *EthAdapter) FullName() string {
	return a.fullName
}

// Decimal 精度
func (a *EthAdapter) Decimal() int32 {
	return a.decimals
}

// CurveType 曲线
func (a *EthAdapter) CurveType() uint32 {
	return a.wm.Config.CurveType
}

// BalanceModelType 按地址
func (a *EthAdapter) BalanceModelType() types.BalanceModelType {
	return types.BalanceModelTypeAddress
}

// GetTransactionDecoder 返回交易解码器
func (a *EthAdapter) GetTransactionDecoder() decoder.TransactionDecoder {
	return a.txDec
}

// GetBlockScanner 暂不实现扫块，返回 nil
func (a *EthAdapter) GetBlockScanner() scanner.BlockScanner {
	return nil
}

// GetAddressDecoder 返回地址解码器
func (a *EthAdapter) GetAddressDecoder() decoder.AddressDecoder {
	return a.addrDec
}

// LoadAssetsConfig 加载配置（如 serverAPI、broadcastAPI、chainID 等）
func (a *EthAdapter) LoadAssetsConfig(config interface{}) error {
	return nil
}

// InitAssetsConfig 初始化默认配置
func (a *EthAdapter) InitAssetsConfig() (interface{}, error) {
	return nil, nil
}

// SetClient 设置 RPC 客户端
func (a *EthAdapter) SetClient(client *rpc.Client) {
	a.wm.Client = client
}

// Config 返回链配置（即 wm.Config）
func (a *EthAdapter) Config() *config.WalletConfig {
	return a.wm.Config
}

// SetNetworkChainID 从节点获取 chainId 并写入 Config
func (a *EthAdapter) SetNetworkChainID() (uint64, error) {
	if a.wm.Client == nil {
		return 0, nil
	}
	return a.wm.SetNetworkChainID()
}

// GetAddrBalance 查询地址原生币余额（测试/查询用）
func (a *EthAdapter) GetAddrBalance(addr, blockTag string) (*big.Int, error) {
	if a.wm.Client == nil {
		return nil, nil
	}
	return a.wm.GetAddrBalance(addr, blockTag)
}

// GetTransactionCount 查询地址 nonce（测试/查询用）
func (a *EthAdapter) GetTransactionCount(addr string) (uint64, error) {
	if a.wm.Client == nil {
		return 0, nil
	}
	return a.wm.GetTransactionCount(addr)
}
