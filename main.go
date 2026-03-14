// main 为 wallet-adapter-eth 的测试入口：从内嵌 INI 通过 eth.NewAdapter 创建并注册适配器，打印链信息与可选地址余额/nonce 后阻塞运行。可选 -addr 指定测试地址。
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/blockchain/wallet-adapter-eth/eth"
	"github.com/blockchain/wallet-adapter/chain"
)

var (
	configContent = `[ETH]
serverAPI = http://127.0.0.1:8547
broadcastAPI = http://127.0.0.1:8547
fixGasLimit =
dataDir = E://test/
fixGasPrice =
offsetsGasPrice = 0
nonceComputeMode = 0
useQNSingleFlightRPC = 0
detectUnknownContracts = 0
moralisAPIKey =
moralisAPIChain = eth
useMoralisAPIParseBlock = 0`
)

var testAddr = flag.String("addr", "0x301db155664284b1462e1a10c082a9ff6e2b617f", "optional: test address for balance/nonce")

func main() {
	adapter, err := eth.NewAdapter(configContent, "ETH", "ETH", 18)
	if err != nil {
		panic(err)
	}

	fmt.Println("--- wallet-adapter-eth 已启动 ---")
	fmt.Printf("Symbol: %s, FullName: %s, Decimal: %d\n", adapter.Symbol(), adapter.FullName(), adapter.Decimal())
	fmt.Printf("ChainID: %d, ServerAPI: %s\n", adapter.Config().ChainID, adapter.Config().ServerAPI)

	decoder, _ := chain.GetTransactionDecoder("ETH")
	if decoder != nil {
		if feeRate, unit, err := decoder.GetRawTransactionFeeRate(nil); err == nil {
			fmt.Printf("Gas 费率: %s %s\n", feeRate, unit)
		}
	}

	if *testAddr != "" {
		bal, err := adapter.GetAddrBalance(*testAddr, "latest")
		if err != nil {
			log.Printf("GetAddrBalance(%s): %v", *testAddr, err)
		} else {
			fmt.Printf("地址 %s 余额(wei): %s\n", *testAddr, bal.String())
		}
		nonce, err := adapter.GetTransactionCount(*testAddr)
		if err != nil {
			log.Printf("GetTransactionCount(%s): %v", *testAddr, err)
		} else {
			fmt.Printf("地址 %s nonce: %d\n", *testAddr, nonce)
		}
	}

	fmt.Println("--- 进程阻塞运行中 (Ctrl+C 退出) ---")
	select {}

}
