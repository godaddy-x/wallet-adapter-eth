package util

import "strings"

// Append0x 地址加 0x 前缀（EVM 等），避免 decoder 与 manager 循环依赖，由 util 提供
func Append0x(addr string) string {
	if strings.HasPrefix(addr, "0x") {
		return addr
	}
	return "0x" + addr
}
