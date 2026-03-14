// Package util 提供通用工具：精度换算（StringToBigInt、BigIntToDecimal）与地址前缀 Append0x（避免 decoder 与 manager 循环依赖）。
package util

import (
	"math/big"
	"strconv"
	"strings"
)

// StringToBigInt 将带小数位的字符串转为 big.Int（amountStr * 10^decimal）
func StringToBigInt(amountStr string, decimal int32) (*big.Int, error) {
	amountStr = strings.TrimSpace(amountStr)
	if amountStr == "" {
		return big.NewInt(0), nil
	}
	parts := strings.Split(amountStr, ".")
	var intPart, fracPart string
	intPart = parts[0]
	if len(parts) > 1 {
		fracPart = parts[1]
	}
	if intPart == "" {
		intPart = "0"
	}
	exp := int(decimal)
	if len(fracPart) > exp {
		fracPart = fracPart[:exp]
	} else {
		for len(fracPart) < exp {
			fracPart += "0"
		}
	}
	combined := intPart + fracPart
	if combined == "" || combined == "0" {
		return big.NewInt(0), nil
	}
	n := new(big.Int)
	_, ok := n.SetString(combined, 10)
	if !ok {
		return nil, strconv.ErrSyntax
	}
	return n, nil
}

// BigIntToDecimal big.Int 转为带精度的字符串（除以 10^decimal）
func BigIntToDecimal(b *big.Int, decimal int32) string {
	if b == nil {
		return "0"
	}
	if decimal <= 0 {
		return b.String()
	}
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimal)), nil)
	q, r := new(big.Int).QuoRem(b, divisor, new(big.Int))
	if r.Sign() == 0 {
		return q.String()
	}
	frac := r.String()
	for len(frac) < int(decimal) {
		frac = "0" + frac
	}
	for len(frac) > 0 && frac[len(frac)-1] == '0' {
		frac = frac[:len(frac)-1]
	}
	if frac == "" {
		return q.String()
	}
	return q.String() + "." + frac
}
