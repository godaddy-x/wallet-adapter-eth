// Package manager 提供 WalletManager：聚合 RPC 客户端与链配置，负责 nonce、gas、余额查询与原始交易广播，供 TransactionDecoder 使用。
package manager

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/godaddy-x/wallet-adapter-eth/internal/config"
	"github.com/godaddy-x/wallet-adapter-eth/internal/models"
	"github.com/godaddy-x/wallet-adapter-eth/internal/rpc"
	"github.com/godaddy-x/wallet-adapter-eth/internal/util"
	adapterconfig "github.com/godaddy-x/wallet-adapter/config"
	"github.com/godaddy-x/wallet-adapter/types"
	"github.com/godaddy-x/wallet-adapter/wallet"
)

// WalletManager 以太坊链能力聚合：RPC、nonce、gas、余额、广播（供 TransactionDecoder 使用）
type WalletManager struct {
	Client *rpc.Client
	Config *config.WalletConfig
}

// LoadAssetsConfig 从 github.com/godaddy-x/wallet-adapter 的 Configer 回调加载并应用配置，初始化 RPC 客户端。
// 会更新 Config、创建数据目录、Dial 并设置 Client；若未配置 chainID 则从节点拉取。
func (wm *WalletManager) LoadAssetsConfig(c adapterconfig.Configer) error {
	symbol := "ETH"
	if wm.Config != nil {
		symbol = wm.Config.Symbol
	}
	cfg := config.BuildConfigFromConfiger(c, symbol)
	wm.Config = cfg
	cfg.MakeDataDir()

	client, err := rpc.Dial(cfg.ServerAPI, cfg.BroadcastAPI)
	if err != nil {
		return err
	}
	wm.Client = client

	if cfg.ChainID == 0 {
		_, _ = wm.SetNetworkChainID()
	}
	return nil
}

// GetTransactionCount 获取地址 nonce
func (wm *WalletManager) GetTransactionCount(addr string) (uint64, error) {
	addr = util.Append0x(addr)
	result, err := wm.Client.Call("eth_getTransactionCount", []interface{}{addr, "latest"})
	if err != nil {
		return 0, err
	}
	return hexutil.DecodeUint64(result.String())
}

// GetAddrBalance 获取原生币余额
func (wm *WalletManager) GetAddrBalance(addr, blockTag string) (*big.Int, error) {
	addr = util.Append0x(addr)
	if blockTag == "" {
		blockTag = "pending"
	}
	result, err := wm.Client.Call("eth_getBalance", []interface{}{addr, blockTag})
	if err != nil {
		return nil, err
	}
	return hexutil.DecodeBig(result.String())
}

// GetGasPrice 当前 gas price
func (wm *WalletManager) GetGasPrice() (*big.Int, error) {
	result, err := wm.Client.Call("eth_gasPrice", nil)
	if err != nil {
		return nil, err
	}
	return hexutil.DecodeBig(result.String())
}

// GetGasEstimated 估算 gas
func (wm *WalletManager) GetGasEstimated(from, to string, value *big.Int, data []byte) (*big.Int, error) {
	msg := map[string]interface{}{
		"from": util.Append0x(from),
		"to":   util.Append0x(to),
		"data": hexutil.Encode(data),
	}
	if value != nil {
		msg["value"] = hexutil.EncodeBig(value)
	}
	result, err := wm.Client.Call("eth_estimateGas", []interface{}{msg})
	if err != nil {
		return nil, err
	}
	return hexutil.DecodeBig(result.String())
}

// GetTransactionFeeEstimated 估算手续费（gasLimit * gasPrice）
func (wm *WalletManager) GetTransactionFeeEstimated(from, to string, value *big.Int, data []byte) (*models.TxFeeInfo, error) {
	wm.Config.EnsureBigInt()
	var gasLimit, gasPrice *big.Int
	var err error
	if wm.Config.FixGasLimit.Cmp(big.NewInt(0)) > 0 {
		gasLimit = wm.Config.FixGasLimit
	} else {
		gasLimit, err = wm.GetGasEstimated(from, to, value, data)
		if err != nil {
			return nil, err
		}
		if data != nil && len(data) > 0 {
			gasLimit = new(big.Int).Mul(gasLimit, big.NewInt(110))
			gasLimit = gasLimit.Div(gasLimit, big.NewInt(100))
		}
	}
	if wm.Config.FixGasPrice.Cmp(big.NewInt(0)) > 0 {
		gasPrice = wm.Config.FixGasPrice
	} else {
		gasPrice, err = wm.GetGasPrice()
		if err != nil {
			return nil, err
		}
		gasPrice = new(big.Int).Add(gasPrice, wm.Config.OffsetsGasPrice)
	}
	fee := &models.TxFeeInfo{GasLimit: gasLimit, GasPrice: gasPrice}
	fee.CalcFee()
	return fee, nil
}

// SendRawTransaction 广播已签名交易
func (wm *WalletManager) SendRawTransaction(signedHex string) (string, error) {
	result, err := wm.Client.Call("eth_sendRawTransaction", []interface{}{signedHex})
	if err != nil {
		return "", err
	}
	return result.String(), nil
}

// SetNetworkChainID 从节点获取 chainId 并写入 Config
func (wm *WalletManager) SetNetworkChainID() (uint64, error) {
	result, err := wm.Client.Call("eth_chainId", nil)
	if err != nil {
		return 0, err
	}
	id, err := hexutil.DecodeUint64(result.String())
	if err != nil {
		return 0, err
	}
	wm.Config.ChainID = id
	return id, nil
}

// SymbolDecimal 链原生精度
func (wm *WalletManager) SymbolDecimal() int32 {
	return 18
}

// EthCall eth_call
func (wm *WalletManager) EthCall(msg models.CallMsg, blockTag string) (string, error) {
	if msg.Value == nil {
		msg.Value = big.NewInt(0)
	}
	if blockTag == "" {
		blockTag = "latest"
	}
	param := map[string]interface{}{
		"from":  msg.From.String(),
		"to":    msg.To.String(),
		"value": hexutil.EncodeBig(msg.Value),
		"data":  hexutil.Encode(msg.Data),
	}
	result, err := wm.Client.Call("eth_call", []interface{}{param, blockTag})
	if err != nil {
		return "", err
	}
	return result.String(), nil
}

// GetAddressNonce 获取用于建交易的 nonce（支持 wrapper 扩展存储自增 nonce）
func (wm *WalletManager) GetAddressNonce(wrapper wallet.WalletDAI, address string) uint64 {
	const keySuffix = "-nonce"
	key := wm.Config.Symbol + keySuffix
	var nonce uint64
	if wm.Config.NonceComputeMode == 0 && wrapper != nil {
		if v, _ := wrapper.GetAddressExtParam(address, key); v != nil {
			if n, ok := parseUint64(v); ok {
				nonce = n
			}
		}
	}
	onchain, err := wm.GetTransactionCount(address)
	if err != nil {
		return nonce
	}
	if nonce > onchain {
		return nonce
	}
	return onchain
}

// UpdateAddressNonce 更新地址扩展 nonce（广播成功后调用）
func (wm *WalletManager) UpdateAddressNonce(wrapper wallet.WalletDAI, address string, nonce uint64) {
	if wrapper == nil {
		return
	}
	key := wm.Config.Symbol + "-nonce"
	_ = wrapper.SetAddressExtParam(address, key, nonce)
}

func parseUint64(v interface{}) (uint64, bool) {
	switch n := v.(type) {
	case uint64:
		return n, true
	case int:
		if n >= 0 {
			return uint64(n), true
		}
	case float64:
		if n >= 0 {
			return uint64(n), true
		}
	}
	return 0, false
}

// ConvertError 转为 types.AdapterError
func ConvertError(err error) *types.AdapterError {
	return types.ConvertError(err)
}

// NewError 创建 AdapterError
func NewError(code uint64, msg string) *types.AdapterError {
	return types.NewError(code, msg)
}

// Errorf 格式化 AdapterError
func Errorf(code uint64, format string, a ...interface{}) *types.AdapterError {
	return types.Errorf(code, format, a...)
}
