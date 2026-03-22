// Module github.com/godaddy-x/wallet-adapter-eth：以太坊及 EVM 兼容链的 wallet-adapter 子类，提供 ChainAdapter、TransactionDecoder、AddressDecoder、SmartContractDecoder 及 LoadAssetsConfig 配置（支持 JSON/INI 等多种格式）。
module github.com/godaddy-x/wallet-adapter-eth

go 1.26

require (
	github.com/ethereum/go-ethereum v1.10.17
	github.com/godaddy-x/wallet-adapter v0.0.0
	github.com/imroc/req v0.3.2
	github.com/tidwall/gjson v1.9.3
	golang.org/x/crypto v0.49.0
)

require (
	github.com/StackExchange/wmi v0.0.0-20180116203802-5d049714c4a6 // indirect
	github.com/btcsuite/btcd/btcec/v2 v2.1.2 // indirect
	github.com/deckarep/golang-set v1.8.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/go-ole/go-ole v1.2.1 // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.9.1 // indirect
	github.com/shirou/gopsutil v3.21.4-0.20210419000835-c7a38de76ee5+incompatible // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tklauser/go-sysconf v0.3.5 // indirect
	github.com/tklauser/numcpus v0.2.2 // indirect
	golang.org/x/sys v0.42.0 // indirect
	gopkg.in/natefinch/npipe.v2 v2.0.0-20160621034901-c1b8fa8bdcce // indirect
)

replace github.com/godaddy-x/wallet-adapter => ../wallet-adapter
