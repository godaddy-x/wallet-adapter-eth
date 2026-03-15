// Package decoder 实现 github.com/godaddy-x/wallet-adapter 的 decoder 分类：AddressDecoder（EVM 地址）与 TransactionDecoder（交易建单/验签/广播）。
// 本文件为 AddressDecoder 实现（EVM 0x+20 字节）。
package decoder

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/godaddy-x/wallet-adapter/decoder"
	"github.com/godaddy-x/wallet-adapter-eth/internal/util"
	"golang.org/x/crypto/sha3"
)

// EthAddressDecoder 实现 decoder.AddressDecoder（EVM 0x+20 字节）
type EthAddressDecoder struct {
	decoder.AddressDecoderBase
}

// DefaultDecoder 默认单例
var DefaultDecoder = &EthAddressDecoder{}

// AddressDecode 地址解码为 20 字节
func (d *EthAddressDecoder) AddressDecode(addr string, _ ...interface{}) ([]byte, error) {
	addr = strings.TrimPrefix(addr, "0x")
	return hex.DecodeString(addr)
}

// AddressEncode 公钥(65 字节 04||X||Y) Keccak256 取后 20 字节
func (d *EthAddressDecoder) AddressEncode(pub []byte, _ ...interface{}) (string, error) {
	if len(pub) != 65 || pub[0] != 0x04 {
		return "", fmt.Errorf("invalid uncompressed public key")
	}
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write(pub[1:])
	hash := hasher.Sum(nil)
	return "0x" + hex.EncodeToString(hash[12:]), nil
}

// AddressVerify 校验 0x + 20 字节 hex
func (d *EthAddressDecoder) AddressVerify(addr string, _ ...interface{}) bool {
	if addr == "" || !strings.HasPrefix(addr, "0x") {
		return false
	}
	b, err := hex.DecodeString(addr[2:])
	return err == nil && len(b) == 20
}

// PublicKeyToAddress 公钥转地址（EVM）
func (d *EthAddressDecoder) PublicKeyToAddress(pub []byte, _ bool) (string, error) {
	return d.AddressEncode(pub)
}

// Append0x 地址加 0x 前缀，内部委托 util.Append0x，供 decoder 包内 transaction 等使用。
func Append0x(addr string) string {
	return util.Append0x(addr)
}
