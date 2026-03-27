package eth

import (
	"math/big"

	adapter "github.com/godaddy-x/wallet-adapter"
	"github.com/godaddy-x/wallet-adapter-eth/internal/config"
	"github.com/godaddy-x/wallet-adapter-eth/internal/decoder"
	"github.com/godaddy-x/wallet-adapter-eth/internal/manager"
	"github.com/godaddy-x/wallet-adapter-eth/internal/rpc"
	ethscanner "github.com/godaddy-x/wallet-adapter-eth/internal/scanner"
	"github.com/godaddy-x/wallet-adapter/chain"
	adapterconfig "github.com/godaddy-x/wallet-adapter/config"
	"github.com/godaddy-x/wallet-adapter/scanner"
	"github.com/godaddy-x/wallet-adapter/types"
)

// EthAdapter 实现 github.com/godaddy-x/wallet-adapter 的 ChainAdapter，用于 ETH 及 EVM 兼容链（BSC、Polygon、Arbitrum 等）
type EthAdapter struct {
	chain.ChainAdapterBase
	wm          *manager.WalletManager
	txDec       *decoder.EthTransactionDecoder
	addrDec     *decoder.EthAddressDecoder
	contractDec *decoder.EthSmartContractDecoder
	blockScan   scanner.BlockScanner
	symbol      string
	fullName    string
	decimals    int32
}

// NewEthAdapter 创建以太坊链适配器（仅设 symbol/fullName/decimals，Config 与 RPC 客户端由 LoadAssetsConfig 回调初始化）。
func NewEthAdapter(symbol, fullName string, decimals int32) *EthAdapter {
	cfg := config.NewConfig(symbol)
	wm := &manager.WalletManager{Config: cfg, Client: nil}
	txDec := decoder.NewTransactionDecoder(wm)
	addrDec := decoder.DefaultDecoder
	contractDec := decoder.NewSmartContractDecoder(wm)
	blockScan := ethscanner.NewBlockScanner(wm)
	return &EthAdapter{
		wm:          wm,
		txDec:       txDec,
		addrDec:     addrDec,
		contractDec: contractDec,
		blockScan:   blockScan,
		symbol:      symbol,
		fullName:    fullName,
		decimals:    decimals,
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
func (a *EthAdapter) GetTransactionDecoder() adapter.TransactionDecoder {
	return a.txDec
}

func (a *EthAdapter) GetBlockScanner() scanner.BlockScanner {
	return a.blockScan
}

// GetAddressDecoder 返回地址解码器
func (a *EthAdapter) GetAddressDecoder() adapter.AddressDecoder {
	return a.addrDec
}

// GetSmartContractDecoder 返回智能合约解码器（ERC20 代币余额与元数据；Call/Create/Submit 暂未实现）
func (a *EthAdapter) GetSmartContractDecoder() adapter.SmartContractDecoder {
	return a.contractDec
}

// LoadAssetsConfig 从外部配置回调加载并应用参数（如 serverAPI、broadcastAPI、chainID、gas、nonce 策略等）。
// cfg 可为实现 github.com/godaddy-x/wallet-adapter/config.Configer 接口的对象（支持 JSON/INI 等格式），或 map[string]string（key 小写匹配）。
// 委托 wm.LoadAssetsConfig 更新 Config、MakeDataDir、并初始化 RPC 客户端；未配 chainID 时从节点拉取。
func (a *EthAdapter) LoadAssetsConfig(cfg interface{}) error {
	var c adapterconfig.Configer
	switch v := cfg.(type) {
	case adapterconfig.Configer:
		c = v
	case map[string]string:
		c = adapterconfig.MapConfig(v)
	default:
		return nil
	}
	return a.wm.LoadAssetsConfig(c)
}

// InitAssetsConfig 返回默认配置占位，供框架合并或展示；实际配置建议通过 JSON/INI + LoadAssetsConfig 或 NewAdapter(jsonContent, ...) 提供。
func (a *EthAdapter) InitAssetsConfig() (interface{}, error) {
	return map[string]string{}, nil
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
