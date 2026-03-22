# wallet-adapter-eth

**模块路径**：`github.com/godaddy-x/wallet-adapter-eth`

以太坊及 EVM 兼容链的 [wallet-adapter](https://github.com/godaddy-x/wallet-adapter) 子类实现，为 BSC、Polygon、Arbitrum 等提供统一 ChainAdapter。支持原生币与 ERC20 建单、EIP-155 签名、原始交易广播，可与外部 MPC 签名服务配合使用；配置通过 LoadAssetsConfig 回调传入，支持 wallet-adapter/config 定义的多种格式（JSON/INI 等）。

## 概述

- **基础框架**：[wallet-adapter](https://github.com/godaddy-x/wallet-adapter) 的 `ChainAdapter`（SymbolInfo、AssetsConfig、TransactionDecoder、BlockScanner、AddressDecoder）。
- **实现要点**：使用 github.com/godaddy-x/wallet-adapter 的 `types.RawTransaction`、`wallet.WalletDAI` 等；实现 `decoder.TransactionDecoder`、`decoder.AddressDecoder`、`decoder.SmartContractDecoder`；EVM 链配置（WalletConfig）在 internal/config，通过 wallet-adapter/config 的 Configer 接口加载，支持 JSON/INI 等多种格式。

## 项目结构（按功能划分 package）

```
wallet-adapter-eth/
├── go.mod
├── go.sum
├── main.go              # 入口（从 JSON 配置一行启动）
├── README.md
├── LICENSE
├── resource/
│   └── config.json      # 示例配置（JSON 格式）
├── eth/                 # 对外包：适配器与一行启动
│   ├── doc.go
│   ├── adapter.go       # EthAdapter、NewEthAdapter
│   └── run.go           # NewAdapter(jsonContent, symbol, fullName, decimals)
└── internal/            # 内部包（仅本模块使用）
    ├── config/          # WalletConfig（EVM）、BuildConfigFromConfiger（使用 wallet-adapter/config 的 Configer 接口）
    ├── decoder/         # AddressDecoder、TransactionDecoder、SmartContractDecoder（对应 github.com/godaddy-x/wallet-adapter/decoder）
    │   ├── address.go
    │   ├── transaction.go
    │   └── contract.go
    ├── manager/         # WalletManager（RPC、nonce、gas、余额、广播）
    │   └── manager.go
    ├── models/          # TxFeeInfo、AddrBalance、CallMsg
    │   └── models.go
    ├── rpc/             # JSON-RPC 客户端（Dial、Call、广播）
    │   └── client.go
    └── util/            # StringToBigInt、BigIntToDecimal、Append0x
        ├── addr.go
        └── decimal.go
```

## 依赖

- Go 1.26+
- [github.com/godaddy-x/wallet-adapter](https://github.com/godaddy-x/wallet-adapter)（提供 Configer 接口、MapConfig 实现及配置解析能力；本地开发可用 replace 指向 `../wallet-adapter`）
- [github.com/ethereum/go-ethereum](https://github.com/ethereum/go-ethereum)
- [github.com/imroc/req](https://github.com/imroc/req)、[github.com/tidwall/gjson](https://github.com/tidwall/gjson)

## 使用方式

### 1. 一行启动（从 JSON 内容创建并注册适配器）

```go
import (
    "github.com/godaddy-x/wallet-adapter-eth/eth"
)

// jsonContent 为 JSON 格式的配置字符串，通过 wallet-adapter/config 的 MapConfig 解析
// 支持字段：serverAPI、broadcastAPI、dataDir、chainID、fixGasLimit、fixGasPrice 等
adapter, err := eth.NewAdapter(jsonContent, "ETH", "Ethereum", 18)
if err != nil {
    return err
}
// adapter 即 *eth.EthAdapter，已注册到 chain，并已设置 RPC 客户端；若 ChainID 为 0 会从节点拉取
```

### 2. 设置 RPC 并获取解码器

```go
import (
    "github.com/godaddy-x/wallet-adapter/chain"
    "github.com/godaddy-x/wallet-adapter-eth/eth"
    "github.com/godaddy-x/wallet-adapter-eth/internal/rpc"
)

// 连接节点（读写可分离：广播走 BroadcastURL）
client, err := rpc.Dial("https://eth-mainnet.g.alchemy.com/v2/xxx", "https://eth-mainnet.g.alchemy.com/v2/yyy")
if err != nil {
    return err
}
defer client.Close()

a, _ := chain.GetAdapter("ETH")
if e, ok := a.(*eth.EthAdapter); ok {
    e.SetClient(client)
    _, _ = e.SetNetworkChainID() // 从节点拉取 chainId 并写入 Config
}

decoder, _ := chain.GetTransactionDecoder("ETH")
addressDecoder, _ := chain.GetAddressDecoder("ETH")
```

### 3. 构建与广播交易（配合 flow + WalletDAI）

```go
import (
    "github.com/godaddy-x/wallet-adapter/chain"
    "github.com/godaddy-x/wallet-adapter/flow"
)

// wrapper 需实现 wallet.WalletDAI（SignPendingTxData、GetAddressList 等）
pending, err := flow.BuildTransaction(decoder, wrapper, rawTx)
// ... MPC 签名填充 pending.SignerList ...
tx, err := flow.SendTransaction(decoder, wrapper, pending)
```

### 4. 多链（BSC、Polygon、Arbitrum）

为同一套代码支持多链，使用 `NewEthAdapter(symbol, fullName, decimals)` 创建后调用 `LoadAssetsConfig(c)` 初始化配置与 RPC（c 为 github.com/godaddy-x/wallet-adapter/config.Configer 或 map[string]string）：

```go
import (
    "github.com/godaddy-x/wallet-adapter/chain"
    "github.com/godaddy-x/wallet-adapter/config"
    "github.com/godaddy-x/wallet-adapter-eth/eth"
)

func registerEvmChains() {
    chains := []struct{ symbol, fullName, api string }{
        {"ETH", "Ethereum", "https://eth.llamarpc.com"},
        {"BSC", "BNB Smart Chain", "https://bsc-dataseed.binance.org"},
        {"MATIC", "Polygon", "https://polygon-rpc.com"},
        {"ARB", "Arbitrum One", "https://arb1.arbitrum.io/rpc"},
    }
    for _, c := range chains {
        adapter := eth.NewEthAdapter(c.symbol, c.fullName, 18)
        chain.RegAdapter(c.symbol, adapter)
        kv := map[string]string{"serverAPI": c.api, "broadcastAPI": c.api}
        _ = adapter.LoadAssetsConfig(config.MapConfig(kv))
    }
}
```

## 当前能力

| 能力          | 说明                                                                                             |
| ------------- | ------------------------------------------------------------------------------------------------ |
| 原生 ETH 转账 | CreateRawTransaction / SubmitRawTransaction / VerifyRawTransaction                               |
| ERC20 转账    | CreateRawTransaction（IsContract 时建 transfer 单）/ SubmitRawTransaction / VerifyRawTransaction |
| 费率          | GetRawTransactionFeeRate、EstimateRawTransactionFee                                              |
| 汇总          | CreateSummaryRawTransactionWithError（仅原生币）                                                 |
| 地址          | AddressDecode / AddressEncode / AddressVerify、PublicKeyToAddress（EVM 0x+20 字节）              |
| MPC 签名      | 支持 64 字节 R\|S 转 65 字节 R\|S\|V，EIP-155 chainId                                            |
| BlockScanner  | 暂未实现（返回 nil），可按需扩展                                                                 |

## 本地测试（main + JSON）

根目录 `main.go` 内置 JSON 示例并阻塞运行，可选 `-addr` 测试指定地址的余额与 nonce：

```bash
# 直接运行（使用代码内嵌 JSON 配置）
go run .

# 带地址时打印该地址余额和 nonce
go run . -addr 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
```

JSON 配置示例见 `resource/config.json`，需配置 `serverAPI`、`broadcastAPI`、`dataDir` 等字段。

## 文档与包说明

| 位置                       | 说明                                                                                         |
| -------------------------- | -------------------------------------------------------------------------------------------- |
| 本 README                  | 项目概述、结构、依赖、使用方式与能力表                                                       |
| eth/doc.go                 | 对外包 eth 的包文档与使用示例                                                                |
| internal/config            | EVM WalletConfig、BuildConfigFromConfiger（依赖 github.com/godaddy-x/wallet-adapter/config） |
| internal/manager           | WalletManager、LoadAssetsConfig、RPC/余额/广播                                               |
| internal/decoder           | AddressDecoder、TransactionDecoder、SmartContractDecoder（EVM/ERC20/ERC20 代币余额与元数据） |
| internal/rpc、models、util | JSON-RPC 客户端、内部模型、精度与地址工具                                                    |

## License

MIT
