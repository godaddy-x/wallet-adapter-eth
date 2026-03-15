// 本文件为 decoder 包中的 SmartContractDecoder 实现（ERC20 代币余额与元数据），对应 wallet-adapter 的 decoder.SmartContractDecoder 接口。
package decoder

import (
	"fmt"
	"sync"

	"github.com/blockchain/wallet-adapter-eth/internal/manager"
	"github.com/blockchain/wallet-adapter-eth/internal/util"
	adapterdecoder "github.com/blockchain/wallet-adapter/decoder"
	"github.com/blockchain/wallet-adapter/types"
	"github.com/blockchain/wallet-adapter/wallet"
)

// EthSmartContractDecoder 实现 adapterdecoder.SmartContractDecoder（ERC20 等），依赖 WalletManager 做 RPC 与 ABI 调用。
type EthSmartContractDecoder struct {
	adapterdecoder.SmartContractDecoderBase
	Wm *manager.WalletManager

	abiMapMu sync.RWMutex
	abiMap   map[string]types.ABIInfo
}

// NewSmartContractDecoder 创建智能合约解码器
func NewSmartContractDecoder(wm *manager.WalletManager) *EthSmartContractDecoder {
	return &EthSmartContractDecoder{Wm: wm, abiMap: make(map[string]types.ABIInfo)}
}

// GetTokenBalanceByAddress 查询地址代币余额列表（ERC20 balanceOf）
func (d *EthSmartContractDecoder) GetTokenBalanceByAddress(contract types.SmartContract, address ...string) ([]*types.TokenBalance, error) {
	if d.Wm == nil || d.Wm.Client == nil {
		return nil, fmt.Errorf("wallet manager or client is nil")
	}
	contractAddr := util.Append0x(contract.Address)
	decimals := int32(contract.Decimals)
	if decimals < 0 {
		decimals = 18
	}
	result := make([]*types.TokenBalance, 0, len(address))
	for _, addr := range address {
		bal, err := d.Wm.ERC20BalanceOf(contractAddr, addr)
		if err != nil {
			continue
		}
		bStr := util.BigIntToDecimal(bal, decimals)
		tb := &types.TokenBalance{
			Contract: &contract,
			Balance: &types.Balance{
				Symbol:           contract.Symbol,
				Address:          addr,
				Balance:          bStr,
				ConfirmBalance:   bStr,
				UnconfirmBalance: "0",
			},
		}
		result = append(result, tb)
	}
	return result, nil
}

// GetTokenMetadata 根据合约地址查询代币元数据（ERC20 name/symbol/decimals）
func (d *EthSmartContractDecoder) GetTokenMetadata(contractAddr string) (*types.SmartContract, error) {
	if d.Wm == nil || d.Wm.Client == nil {
		return nil, fmt.Errorf("wallet manager or client is nil")
	}
	contractAddr = util.Append0x(contractAddr)
	name, symbol, decimals, err := d.Wm.ERC20Metadata(contractAddr)
	if err != nil {
		return nil, err
	}
	contractID := contractAddr
	if d.Wm.Config != nil && d.Wm.Config.Symbol != "" {
		contractID = d.Wm.Config.Symbol + "_" + contractAddr
	}
	sc := &types.SmartContract{
		ContractID: contractID,
		Symbol:     d.Wm.Config.Symbol,
		Address:    contractAddr,
		Token:      symbol,
		Name:       name,
		Decimals:   uint64(decimals),
	}
	return sc, nil
}

// GetABIInfo 返回已缓存的合约 ABI 信息
func (d *EthSmartContractDecoder) GetABIInfo(address string) (*types.ABIInfo, error) {
	d.abiMapMu.RLock()
	defer d.abiMapMu.RUnlock()
	info, ok := d.abiMap[util.Append0x(address)]
	if !ok {
		return nil, nil
	}
	return &info, nil
}

// SetABIInfo 缓存合约 ABI 信息
func (d *EthSmartContractDecoder) SetABIInfo(address string, abi types.ABIInfo) error {
	d.abiMapMu.Lock()
	defer d.abiMapMu.Unlock()
	if d.abiMap == nil {
		d.abiMap = make(map[string]types.ABIInfo)
	}
	d.abiMap[util.Append0x(address)] = abi
	return nil
}

// CallSmartContractABI 只读调用合约 ABI 方法（暂未实现，返回未实现错误）
func (d *EthSmartContractDecoder) CallSmartContractABI(wrapper wallet.WalletDAI, rawTx *types.SmartContractRawTransaction) (*types.SmartContractCallResult, *types.AdapterError) {
	return nil, types.Errorf(types.ErrSystemException, "CallSmartContractABI not implement")
}

// CreateSmartContractRawTransaction 创建智能合约原始交易单（暂未实现）
func (d *EthSmartContractDecoder) CreateSmartContractRawTransaction(wrapper wallet.WalletDAI, rawTx *types.SmartContractRawTransaction) *types.AdapterError {
	return types.Errorf(types.ErrSystemException, "CreateSmartContractRawTransaction not implement")
}

// SubmitSmartContractRawTransaction 广播智能合约交易单（暂未实现）
func (d *EthSmartContractDecoder) SubmitSmartContractRawTransaction(wrapper wallet.WalletDAI, rawTx *types.SmartContractRawTransaction) (*types.SmartContractReceipt, *types.AdapterError) {
	return nil, types.Errorf(types.ErrSystemException, "SubmitSmartContractRawTransaction not implement")
}

// 确保实现 adapterdecoder.SmartContractDecoder
var _ adapterdecoder.SmartContractDecoder = (*EthSmartContractDecoder)(nil)
