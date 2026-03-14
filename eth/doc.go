// Package eth 实现基于 [wallet-adapter](https://github.com/godaddy-x/wallet-adapter) 的以太坊及 EVM 兼容链适配器。
//
// 本包实现 wallet-adapter 的 ChainAdapter 接口，
// 支持离线构建交易、EIP-155 签名消息生成、原始交易广播，并与外部 MPC 签名服务配合使用。
//
// 功能按 internal 子包划分：config（配置与 INI）、rpc（RPC 客户端）、decoder（AddressDecoder + TransactionDecoder 实现）、
// util（精度换算与 Append0x）、models、manager。对外仅暴露 eth 包：EthAdapter、NewEthAdapter、NewAdapter。
//
// 使用方式：
//
//  1. 一行启动（从 INI 内容创建适配器并注册）：
//     adapter, err := eth.NewAdapter(iniContent, "ETH", "Ethereum", 18)
//     ethAdapter := adapter.(*eth.EthAdapter)
//
//  2. 或手动创建并设置 RPC（需 import internal/config 与 internal/rpc）：
//     cfg, client, _ := config.LoadFromINIContent(iniText, "ETH")
//     a := eth.NewEthAdapter(cfg, "ETH", "Ethereum", 18)
//     a.SetClient(client)
//     chain.RegAdapter("ETH", a)
//
//  3. 通过 wallet-adapter 的 flow 构建与广播交易：
//     dec, _ := chain.GetTransactionDecoder("ETH")
//     pending, _ := flow.BuildTransaction(dec, wrapper, rawTx)
//     tx, _ := flow.SendTransaction(dec, wrapper, pending)
package eth
