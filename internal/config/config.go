// Package config 提供链配置 WalletConfig 及从 INI 文件/内容加载（LoadFromINI、LoadFromINIContent、BuildConfigFromKV）。
package config

import (
	"math/big"
	"path/filepath"
	"strings"
)

// WalletConfig 链配置（RPC、Gas、Nonce 策略等）
type WalletConfig struct {
	Symbol           string
	ServerAPI        string
	BroadcastAPI     string
	DataDir          string
	ChainID          uint64
	CurveType        uint32
	FixGasLimit      *big.Int
	FixGasPrice      *big.Int
	OffsetsGasPrice  *big.Int
	NonceComputeMode int64 // 0: 自增 nonce，1: 链上 latest
}

// NewConfig 创建指定 symbol 的默认配置
func NewConfig(symbol string) *WalletConfig {
	c := &WalletConfig{
		Symbol:    symbol,
		CurveType: 1, // secp256k1
	}
	c.ensureBigInt()
	return c
}

func (c *WalletConfig) ensureBigInt() {
	if c.FixGasLimit == nil {
		c.FixGasLimit = big.NewInt(0)
	}
	if c.FixGasPrice == nil {
		c.FixGasPrice = big.NewInt(0)
	}
	if c.OffsetsGasPrice == nil {
		c.OffsetsGasPrice = big.NewInt(0)
	}
}

// EnsureBigInt 供外部保证 big.Int 已初始化
func (c *WalletConfig) EnsureBigInt() {
	c.ensureBigInt()
}

// MakeDataDir 创建数据目录路径
func (c *WalletConfig) MakeDataDir() {
	if c.DataDir == "" {
		c.DataDir = "data"
	}
	c.DataDir = filepath.Join(c.DataDir, strings.ToLower(c.Symbol))
}
