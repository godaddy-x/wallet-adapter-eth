# wallet-adapter-eth

Ethereum Adapter 是 [wallet-adapter](https://github.com/godaddy-x/wallet-adapter) 的子类实现，为以太坊及 EVM 兼容链（如 BSC、Polygon、Arbitrum）提供统一的 ChainAdapter 实现。支持离线构建交易、EIP-155 签名消息生成、原始交易广播，并与外部 MPC 签名服务配合使用。

## 来源与改造说明

- **基础框架**：基于 [wallet-adapter](https://github.com/godaddy-x/wallet-adapter) 的 `ChainAdapter` 接口（SymbolInfo、AssetsConfig、TransactionDecoder、BlockScanner、AddressDecoder）。
- **实现要点**：使用 wallet-adapter 的 `types.RawTransaction`、`types.Transaction`、`wallet.WalletDAI` 等类型；实现 `decoder.TransactionDecoder`、`decoder.AddressDecoder`；依赖 wallet-adapter 与 go-ethereum。

## 项目结构（按功能划分 package）

```
wallet-adapter-eth/
├── go.mod
├── go.sum
├── main.go              # 入口（从 INI 文本一行启动）
├── README.md
├── LICENSE
├── resource/
│   └── config.ini       # 示例配置
├── eth/                 # 对外包：适配器与一行启动
│   ├── doc.go
│   ├── adapter.go       # EthAdapter、NewEthAdapter
│   └── run.go           # NewAdapter(iniContent, symbol, fullName, decimals)
└── internal/            # 内部包（仅本模块使用）
    ├── config/          # WalletConfig、LoadFromINI、LoadFromINIContent、BuildConfigFromKV
    │   ├── config.go
    │   └── config_ini.go
    ├── decoder/         # AddressDecoder + TransactionDecoder（对应 wallet-adapter decoder）
    │   ├── address.go
    │   └── transaction.go
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
- [github.com/godaddy-x/wallet-adapter](https://github.com/godaddy-x/wallet-adapter)（本地开发可用 replace 指向 `../wallet-adapter`）
- [github.com/ethereum/go-ethereum](https://github.com/ethereum/go-ethereum)
- [github.com/imroc/req](https://github.com/imroc/req)、[github.com/tidwall/gjson](https://github.com/tidwall/gjson)

## 使用方式

### 1. 一行启动（从 INI 内容创建并注册适配器）

```go
import (
    "github.com/blockchain/wallet-adapter-eth/eth"
)

// iniContent 可为从文件读取的 INI 文本，或直接写死的配置字符串（见 main.go）
adapter, err := eth.NewAdapter(iniContent, "ETH", "Ethereum", 18)
if err != nil {
    return err
}
// adapter 即 *eth.EthAdapter，已注册到 chain，并已设置 RPC 客户端；若 ChainID 为 0 会从节点拉取
```

### 2. 设置 RPC 并获取解码器

```go
import (
    "github.com/blockchain/wallet-adapter/chain"
    "github.com/blockchain/wallet-adapter-eth/eth"
    "github.com/blockchain/wallet-adapter-eth/internal/rpc"
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
    "github.com/blockchain/wallet-adapter/chain"
    "github.com/blockchain/wallet-adapter/flow"
)

// wrapper 需实现 wallet.WalletDAI（SignPendingTxData、GetAddressList 等）
pending, err := flow.BuildTransaction(decoder, wrapper, rawTx)
// ... MPC 签名填充 pending.SignerList ...
tx, err := flow.SendTransaction(decoder, wrapper, pending)
```

### 4. 多链（BSC、Polygon、Arbitrum）

为同一套代码支持多链，使用 `config.LoadFromINI` / `LoadFromINIContent` 建配置，再 `NewEthAdapter(cfg, symbol, fullName, decimals)` 并注册：

```go
import (
    "github.com/blockchain/wallet-adapter/chain"
    "github.com/blockchain/wallet-adapter-eth/eth"
    "github.com/blockchain/wallet-adapter-eth/internal/config"
    "github.com/blockchain/wallet-adapter-eth/internal/rpc"
)

func registerEvmChains() {
    chains := []struct{ symbol, fullName, api string }{
        {"ETH", "Ethereum", "https://eth.llamarpc.com"},
        {"BSC", "BNB Smart Chain", "https://bsc-dataseed.binance.org"},
        {"MATIC", "Polygon", "https://polygon-rpc.com"},
        {"ARB", "Arbitrum One", "https://arb1.arbitrum.io/rpc"},
    }
    for _, c := range chains {
        cfg := config.NewConfig(c.symbol)
        cfg.ServerAPI = c.api
        cfg.BroadcastAPI = c.api
        client, _ := rpc.Dial(c.api, c.api)
        adapter := eth.NewEthAdapter(cfg, c.symbol, c.fullName, 18)
        adapter.SetClient(client)
        chain.RegAdapter(c.symbol, adapter)
    }
}
```

## 当前能力

| 能力 | 说明 |
|------|------|
| 原生 ETH 转账 | CreateRawTransaction / SubmitRawTransaction / VerifyRawTransaction |
| ERC20 转账 | CreateRawTransaction（IsContract 时建 transfer 单）/ SubmitRawTransaction / VerifyRawTransaction |
| 费率 | GetRawTransactionFeeRate、EstimateRawTransactionFee |
| 汇总 | CreateSummaryRawTransactionWithError（仅原生币） |
| 地址 | AddressDecode / AddressEncode / AddressVerify、PublicKeyToAddress（EVM 0x+20 字节） |
| MPC 签名 | 支持 64 字节 R\|S 转 65 字节 R\|S\|V，EIP-155 chainId |
| BlockScanner | 暂未实现（返回 nil），可按需扩展 |

## 本地测试（main + INI）

根目录 `main.go` 内置 INI 示例并阻塞运行，可选 `-addr` 测试指定地址的余额与 nonce：

```bash
# 直接运行（使用代码内嵌 INI）
go run .

# 带地址时打印该地址余额和 nonce
go run . -addr 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
```

更多 INI 示例见 `resource/config.ini`，需在 `[ETH]` 段配置 `serverAPI`、`broadcastAPI` 等。

## License

MIT
