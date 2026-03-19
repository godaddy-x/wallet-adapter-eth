package scanner

import (
	"strings"

	"github.com/godaddy-x/wallet-adapter/types"
)

// findContractReceipt 在 ContractReceipts slice 中查找指定 key 的 receipt
func findContractReceipt(items []*types.ContractReceiptItem, key string) *types.SmartContractReceipt {
	for _, item := range items {
		if item == nil {
			continue
		}
		// 支持完整匹配或后缀匹配（兼容 txid:contract:logIndex 和 contract:logIndex 两种格式）
		if item.Key == key || strings.HasSuffix(item.Key, key) || strings.HasSuffix(key, item.Key) {
			return item.Receipt
		}
	}
	return nil
}
