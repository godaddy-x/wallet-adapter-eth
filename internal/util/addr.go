package util

import "strings"

// Append0x 地址加 0x 前缀（EVM 等），供 manager/decoder 使用以避免循环依赖。
func Append0x(addr string) string {
	if strings.HasPrefix(addr, "0x") {
		return addr
	}
	return "0x" + addr
}
