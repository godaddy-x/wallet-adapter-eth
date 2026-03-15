// Package eth 实现基于 [wallet-adapter](https://github.com/godaddy-x/wallet-adapter) 的以太坊及 EVM 兼容链适配器。
//
// 实现 ChainAdapter 接口，支持原生币与 ERC20 建单、EIP-155 签名、原始交易广播，可与 MPC 签名服务配合。
// 内部按职责分包：config（EVM WalletConfig）、rpc、decoder、manager、models、util；通用 Configer 与 INI 解析来自 wallet-adapter/config。
// 对外仅暴露本包：EthAdapter、NewEthAdapter、NewAdapter。
//
// 使用方式：
//
//  1. 一行启动（从 INI 内容创建并注册）：
//     adapter, err := eth.NewAdapter(iniContent, "ETH", "Ethereum", 18)
//
//  2. 先创建再通过 LoadAssetsConfig 初始化（与 quorum 一致）：
//     a := eth.NewEthAdapter("ETH", "Ethereum", 18)
//     chain.RegAdapter("ETH", a)
//     a.LoadAssetsConfig(c)  // c 为 wallet-adapter/config.Configer 或 map[string]string
//
//  3. 构建与广播交易（配合 flow + WalletDAI）：
//     dec, _ := chain.GetTransactionDecoder("ETH")
//     pending, _ := flow.BuildTransaction(dec, wrapper, rawTx)
//     tx, _ := flow.SendTransaction(dec, wrapper, pending)
package eth
