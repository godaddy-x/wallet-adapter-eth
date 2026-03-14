package config

import (
	"bufio"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"

	"github.com/blockchain/wallet-adapter-eth/internal/rpc"
)

// LoadFromINI 从 INI 文件加载配置并创建 RPC 客户端。
func LoadFromINI(path string, section string) (*WalletConfig, *rpc.Client, error) {
	if section == "" {
		section = "ETH"
	}
	kv, err := parseINISection(path, section)
	if err != nil {
		return nil, nil, err
	}
	cfg := BuildConfigFromKV(kv, section)
	client, err := rpc.Dial(cfg.ServerAPI, cfg.BroadcastAPI)
	if err != nil {
		return nil, nil, err
	}
	return cfg, client, nil
}

// LoadFromINIContent 从 INI 内容文本加载配置并创建 RPC 客户端（不读文件）。
func LoadFromINIContent(content string, section string) (*WalletConfig, *rpc.Client, error) {
	if section == "" {
		section = "ETH"
	}
	kv, err := parseINIContent(content, section)
	if err != nil {
		return nil, nil, err
	}
	cfg := BuildConfigFromKV(kv, section)
	client, err := rpc.Dial(cfg.ServerAPI, cfg.BroadcastAPI)
	if err != nil {
		return nil, nil, err
	}
	return cfg, client, nil
}

// BuildConfigFromKV 根据 section 下的 key=value 映射构建 WalletConfig（不创建 RPC 客户端）
func BuildConfigFromKV(kv map[string]string, section string) *WalletConfig {
	get := func(k string) string { return strings.TrimSpace(kv[strings.ToLower(k)]) }
	cfg := NewConfig(section)
	cfg.ServerAPI = get("serverAPI")
	cfg.BroadcastAPI = get("broadcastAPI")
	if cfg.BroadcastAPI == "" {
		cfg.BroadcastAPI = cfg.ServerAPI
	}
	cfg.DataDir = get("dataDir")
	if v := get("chainID"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			cfg.ChainID = n
		}
	}
	if v := get("fixGasLimit"); v != "" {
		cfg.FixGasLimit = new(big.Int)
		cfg.FixGasLimit.SetString(v, 10)
	}
	if v := get("fixGasPrice"); v != "" {
		cfg.FixGasPrice = new(big.Int)
		cfg.FixGasPrice.SetString(v, 10)
	}
	if v := get("offsetsGasPrice"); v != "" {
		cfg.OffsetsGasPrice = new(big.Int)
		cfg.OffsetsGasPrice.SetString(v, 10)
	}
	if v := get("nonceComputeMode"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.NonceComputeMode = n
		}
	}
	return cfg
}

func parseINISection(path, section string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseINIReader(f, section)
}

func parseINIContent(content string, section string) (map[string]string, error) {
	return parseINIReader(strings.NewReader(content), section)
}

func parseINIReader(r io.Reader, section string) (map[string]string, error) {
	scanner := bufio.NewScanner(r)
	kv := make(map[string]string)
	var current string
	target := strings.ToLower(section)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if len(line) >= 2 && line[0] == '[' && line[len(line)-1] == ']' {
			current = strings.TrimSpace(strings.ToLower(line[1 : len(line)-1]))
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(strings.ToLower(line[:idx]))
		val := strings.TrimSpace(line[idx+1:])
		if key == "" {
			continue
		}
		if current == target {
			kv[key] = val
		}
	}
	return kv, scanner.Err()
}
