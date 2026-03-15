// Package config 提供本适配器使用的链配置（EVM 系：RPC、Gas、Nonce 等），依赖 wallet-adapter/config 的 Configer 与 INI 解析。
package config

import (
	"math/big"
	"path/filepath"
	"strconv"
	"strings"

	adapterconfig "github.com/blockchain/wallet-adapter/config"
)

// WalletConfig 链配置（RPC、Gas、Nonce 策略等），EVM 子类使用。
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

// NewConfig 创建指定 symbol 的默认配置。
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

// EnsureBigInt 供外部保证 big.Int 已初始化。
func (c *WalletConfig) EnsureBigInt() {
	c.ensureBigInt()
}

// MakeDataDir 创建数据目录路径（DataDir 为空时用 data/<symbol>）。
func (c *WalletConfig) MakeDataDir() {
	if c.DataDir == "" {
		c.DataDir = "data"
	}
	c.DataDir = filepath.Join(c.DataDir, strings.ToLower(c.Symbol))
}

// trimQuoted 去掉首尾空格及可选的成对双引号，避免 INI 中 "E://test/"、"http://..." 等带引号值解析异常。
func trimQuoted(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	return strings.TrimSpace(s)
}

// BuildConfigFromConfiger 从 wallet-adapter 的 Configer 读取并填充 WalletConfig，symbol 作为链标识。
func BuildConfigFromConfiger(c adapterconfig.Configer, symbol string) *WalletConfig {
	cfg := NewConfig(symbol)
	cfg.ServerAPI = trimQuoted(c.String("serverAPI"))
	cfg.BroadcastAPI = trimQuoted(c.String("broadcastAPI"))
	if cfg.BroadcastAPI == "" {
		cfg.BroadcastAPI = cfg.ServerAPI
	}
	cfg.DataDir = trimQuoted(c.String("dataDir"))
	if v := c.String("chainID"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			cfg.ChainID = n
		}
	}
	if v := c.String("fixGasLimit"); v != "" {
		cfg.FixGasLimit = newBigIntFromString(v)
	}
	if v := c.String("fixGasPrice"); v != "" {
		cfg.FixGasPrice = newBigIntFromString(v)
	}
	if v := c.String("offsetsGasPrice"); v != "" {
		cfg.OffsetsGasPrice = newBigIntFromString(v)
	}
	n, _ := c.Int64("nonceComputeMode")
	cfg.NonceComputeMode = n
	return cfg
}

// BuildConfigFromKV 根据 key=value 映射构建 WalletConfig（section 作为 symbol）。
func BuildConfigFromKV(kv map[string]string, section string) *WalletConfig {
	return BuildConfigFromConfiger(adapterconfig.MapConfig(kv), section)
}

func newBigIntFromString(s string) *big.Int {
	s = strings.TrimSpace(s)
	if s == "" {
		return big.NewInt(0)
	}
	z := new(big.Int)
	z.SetString(s, 10)
	return z
}
