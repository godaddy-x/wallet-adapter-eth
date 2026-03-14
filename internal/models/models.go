// Package models 定义内部数据结构：TxFeeInfo、AddrBalance、CallMsg 等，供 manager 与 decoder 使用。
package models

import (
	"math/big"

	ethcom "github.com/ethereum/go-ethereum/common"
)

// TxFeeInfo 手续费信息
type TxFeeInfo struct {
	GasLimit *big.Int
	GasPrice *big.Int
	Fee      *big.Int
}

// CalcFee 计算 Fee = GasLimit * GasPrice
func (f *TxFeeInfo) CalcFee() {
	if f.GasLimit != nil && f.GasPrice != nil {
		f.Fee = new(big.Int).Mul(f.GasLimit, f.GasPrice)
	}
}

// AddrBalance 用于建交易时选用的地址余额
type AddrBalance struct {
	Address      string
	Balance      *big.Int
	TokenBalance *big.Int // ERC20 时使用
}

// CallMsg eth_call 参数
type CallMsg struct {
	From  ethcom.Address
	To    ethcom.Address
	Data  []byte
	Value *big.Int
}
