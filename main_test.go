package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/godaddy-x/wallet-adapter-eth/eth"
	ethscanner "github.com/godaddy-x/wallet-adapter-eth/internal/scanner"
	"github.com/godaddy-x/wallet-adapter/types"
	"github.com/tidwall/gjson"
)

// testIniContent 为本仓库本地节点集成测试的通用配置。
// 如需切换节点端口/数据目录，只需修改此处。
const testIniContent = `[ETH]
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

const testHeight = 83156
const testHeightToken = 88356

// TestStartBlockScanner 演示如何在业务侧完整初始化适配器和扫块器：
// - 通过 eth.NewAdapter 创建适配器（内部构造 EthBlockScanner）；
// - 设置 ScanTargetFunc：告诉扫块器“哪些地址/合约是我关心的扫描目标”（本用例不关心交易提取，因此默认都不匹配）；
// - 设置 TokenMetadataFunc（在 Base 上，由外部业务注入）：提供代币精度等元数据（本用例返回 nil，回退链上元数据查询）；
// - 实际连本地节点：获取 latest 区块头并执行一次 ScanBlock，验证扫块流程可跑通。
func TestStartBlockScanner(t *testing.T) {
	// 复用 main.go 中的 demo 配置（本地节点地址可按需要调整）
	const iniContent = testIniContent

	// 创建适配器（内部会构造 EthBlockScanner，但此时尚未设置 ScanTargetFunc 与 TokenMetadataFunc）
	adapter, err := eth.NewAdapter(iniContent, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}

	// 断言 BlockScanner 类型为我们实现的 EthBlockScanner，以便访问嵌入的 Base 字段
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}

	// 1）设置 ScanTargetFunc：本用例不关心交易提取，因此全部返回 Exist=false
	_ = bs.SetBlockScanTargetFunc(func(target types.ScanTargetParam) types.ScanTargetResult {
		return types.ScanTargetResult{SourceKey: "test", Exist: true}
	})

	// 2）设置 TokenMetadataFunc：本用例返回 nil，表示业务侧不做兜底配置，回退链上 ERC20Metadata（若链上也查不到则会跳过该事件）
	_ = bs.SetTokenMetadataFunc(func(symbol, contractAddr string) *types.SmartContract {
		// 在真实业务中，这里应根据 symbol+contractAddr 查内部资产表，返回已配置的 SmartContract 信息。
		return nil
	})

	// 3）连节点取 latest 区块并扫一次（同步返回结果集）
	start := time.Now()
	header, err := bs.GetCurrentBlockHeader()
	if err != nil {
		t.Fatalf("GetCurrentBlockHeader error: %v", err)
	}
	if header == nil || header.Height == 0 {
		t.Fatalf("invalid header: %+v", header)
	}

	res, err := rawScanner.ScanBlockWithResult(testHeight)
	if err != nil {
		t.Fatalf("ScanBlockWithResult(%d) error: %v", header.Height, err)
	}

	// 0x0a64dbc4c4cee4521ee034af5cf9b6afc03d133f8b5d80b153a8518bc4e27586

	t.Logf("scan latest block ok: height=%d hash=%s txTotal=%d txFailed=%d extracted=%d elapsed=%s",
		res.Height, res.BlockHash, res.TxTotal, res.TxFailed, res.ExtractedTxs, time.Since(start))
}

// TestScanBlockWithResultFlow 验证 ScanBlockWithResult 的同步扫块流程与块内并行提取逻辑可正常工作。
// 该用例依赖本地节点可用（使用与 TestStartBlockScanner 相同的 INI 配置）。
func TestScanBlockWithResultFlow(t *testing.T) {
	const iniContent = testIniContent

	adapter, err := eth.NewAdapter(iniContent, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}

	// 开启块内并行提取（动态配置）
	bs.SetTxExtractConcurrency(20)

	// 为了让扫块器真正跑提取逻辑，这里让 ScanTargetFunc “全部命中”，避免 ExtractData 永远为空。
	_ = bs.SetBlockScanTargetFunc(func(target types.ScanTargetParam) types.ScanTargetResult {
		return types.ScanTargetResult{Exist: true, SourceKey: "test"}
	})
	_ = bs.SetTokenMetadataFunc(func(symbol, contractAddr string) *types.SmartContract {
		// 测试环境不提供业务侧元数据兜底，回退链上 ERC20Metadata；若获取不到 decimals 则跳过该 token 事件
		return nil
	})

	header, err := bs.GetCurrentBlockHeader()
	if err != nil {
		t.Fatalf("GetCurrentBlockHeader error: %v", err)
	}
	if header == nil || header.Height == 0 || header.Hash == "" {
		t.Fatalf("invalid header: %+v", header)
	}

	start := time.Now()
	res, err := rawScanner.ScanBlockWithResult(header.Height)
	if err != nil {
		t.Fatalf("ScanBlockWithResult(%d) error: %v", header.Height, err)
	}
	if res == nil || res.Header == nil {
		t.Fatalf("nil result: %+v", res)
	}
	if res.Height != header.Height || res.BlockHash == "" || res.Header.Hash != res.BlockHash {
		t.Fatalf("unexpected result header/hash: height=%d/%d hash=%q/%q", res.Height, header.Height, res.BlockHash, res.Header.Hash)
	}

	// 基本一致性检查：ExtractedTxs 应至少覆盖返回的条目数量（TxExtractData + 合约回执）
	var extractCount uint64
	for _, list := range res.ExtractData {
		extractCount += uint64(len(list))
	}
	receiptCount := uint64(len(res.ContractReceipts))
	if res.ExtractedTxs < extractCount+receiptCount {
		t.Fatalf("ExtractedTxs too small: extracted=%d < extractData(%d)+receipts(%d)", res.ExtractedTxs, extractCount, receiptCount)
	}

	t.Logf("ScanBlockWithResult ok: height=%d txTotal=%d txFailed=%d extractData=%d receipts=%d elapsed=%s",
		res.Height, res.TxTotal, res.TxFailed, extractCount, receiptCount, time.Since(start))
}

// TestVerifyTransactionByTxID 从 latest 区块取一笔 tx 进行链上复核，验证 VerifyTransactionByTxID 可跑通并返回结果集结构。
func TestVerifyTransactionByTxID(t *testing.T) {
	const iniContent = testIniContent

	adapter, err := eth.NewAdapter(iniContent, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}

	// 全命中，确保能产出结果集（注意：Verify 仍会受“tx 是否成功/确认数”影响）
	_ = bs.SetBlockScanTargetFunc(func(target types.ScanTargetParam) types.ScanTargetResult {
		return types.ScanTargetResult{Exist: true, SourceKey: "test"}
	})
	_ = bs.SetTokenMetadataFunc(func(symbol, contractAddr string) *types.SmartContract { return nil })

	// 从 latest block 中选择一个 txid：这里通过 ScanBlockWithResult 无法拿到 txid，因此直接用扫块器内部 RPC（已通过接口封装）：
	// 使用链上接口：先取 latest header，再用区块号拉区块并取第一笔 tx hash。
	header, err := bs.GetCurrentBlockHeader()
	if err != nil || header == nil || header.Height == 0 {
		t.Fatalf("GetCurrentBlockHeader error: %v header=%+v", err, header)
	}

	// 直接走 HTTP JSON-RPC 拉块，取第一笔 tx hash（避免依赖内部未导出字段）
	body := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{("0x" + strconv.FormatUint(header.Height, 16)), true},
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post("http://127.0.0.1:8547", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("rpc post error: %v", err)
	}
	defer resp.Body.Close()
	var raw bytes.Buffer
	_, _ = raw.ReadFrom(resp.Body)
	js := raw.String()
	txs := gjson.Get(js, "result.transactions").Array()
	if len(txs) == 0 {
		t.Skip("latest block has no txs")
	}
	txid := txs[0].Get("hash").String()
	if txid == "" {
		t.Skip("tx hash empty in latest block")
	}

	vr, err := rawScanner.VerifyTransactionByTxID(txid, bs.ScanTargetFunc, 0)
	if err != nil {
		t.Fatalf("VerifyTransactionByTxID error: %v", err)
	}
	if vr == nil || vr.TxID != txid {
		t.Fatalf("invalid verify result: %+v", vr)
	}
	t.Logf("verify result: verified=%v reason=%q height=%d conf=%d status=%s extractKeys=%d receipts=%d",
		vr.Verified, vr.Reason, vr.BlockHeight, vr.Confirmations, vr.Status, len(vr.ExtractData), len(vr.ContractReceipts))
}

func TestVerifyTransactionMatch(t *testing.T) {
	const iniContent = testIniContent

	adapter, err := eth.NewAdapter(iniContent, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}
	_ = bs.SetBlockScanTargetFunc(func(target types.ScanTargetParam) types.ScanTargetResult {
		return types.ScanTargetResult{Exist: true, SourceKey: "test"}
	})
	_ = bs.SetTokenMetadataFunc(func(symbol, contractAddr string) *types.SmartContract { return nil })

	// 取 latest 区块第一笔 txid
	header, err := bs.GetCurrentBlockHeader()
	if err != nil || header == nil || header.Height == 0 {
		t.Fatalf("GetCurrentBlockHeader error: %v header=%+v", err, header)
	}
	body := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{("0x" + strconv.FormatUint(header.Height, 16)), true},
	}
	b, _ := json.Marshal(body)
	resp, err := http.Post("http://127.0.0.1:8547", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("rpc post error: %v", err)
	}
	defer resp.Body.Close()
	var raw bytes.Buffer
	_, _ = raw.ReadFrom(resp.Body)
	js := raw.String()
	txs := gjson.Get(js, "result.transactions").Array()
	if len(txs) == 0 {
		t.Skip("latest block has no txs")
	}
	txid := txs[0].Get("hash").String()
	if txid == "" {
		t.Skip("tx hash empty in latest block")
	}

	// 先走 VerifyTransactionByTxID 得到链上提取结果，然后用其中一条主币交易单构造 expected（避免依赖未导出字段或重复 RPC）。
	chain, err := rawScanner.VerifyTransactionByTxID(txid, bs.ScanTargetFunc, 0)
	if err != nil {
		t.Fatalf("VerifyTransactionByTxID error: %v", err)
	}
	if chain == nil || !chain.Verified {
		t.Skipf("tx not verified (maybe pending/failed): reason=%q", func() string {
			if chain == nil {
				return "nil"
			}
			return chain.Reason
		}())
	}
	var from, to, amountStr string
found:
	for _, list := range chain.ExtractData {
		for _, d := range list {
			if d == nil || d.Transaction == nil {
				continue
			}
			tx := d.Transaction
			if tx.Coin.IsContract {
				continue
			}
			// 主币 value=0 的交易不会产生交易单，这里拿到的应该都有 amount
			if len(tx.From) > 0 {
				parts := strings.SplitN(tx.From[0], ":", 2)
				from = strings.ToLower(parts[0])
				if len(parts) == 2 {
					amountStr = parts[1]
				}
			}
			if len(tx.To) > 0 {
				parts := strings.SplitN(tx.To[0], ":", 2)
				to = strings.ToLower(parts[0])
			}
			if amountStr == "" {
				amountStr = tx.Amount
			}
			if from != "" && to != "" && amountStr != "" {
				break found
			}
		}
	}
	if from == "" || to == "" || amountStr == "" {
		t.Skip("no native tx extract data found to build expected")
	}

	expected := &types.TxVerifyExpected{
		Symbol:    "ETH",
		TxID:      txid,
		BlockHash: chain.BlockHash,
		Height:    chain.BlockHeight,
		Transfers: []*types.TxTransferExpected{
			{ContractAddr: "", From: from, To: to, Amount: amountStr, Decimals: 0, LogIndex: -1},
		},
	}

	mr, err := rawScanner.VerifyTransactionMatch(txid, expected, bs.ScanTargetFunc, 0)
	if err != nil {
		t.Fatalf("VerifyTransactionMatch error: %v", err)
	}
	if mr == nil {
		t.Fatalf("nil match result")
	}
	t.Logf("match verified=%v reason=%q mismatches=%v", mr.Verified, mr.Reason, mr.Mismatches)
}

// TestScanBlockOnce 测试“指定高度补扫一次”的能力（不进入持续循环）。
func TestScanBlockOnce(t *testing.T) {
	const iniContent = testIniContent

	adapter, err := eth.NewAdapter(iniContent, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}

	// 全命中，确保能产出结果集（若本地节点对应高度无交易也可正常返回）。
	_ = bs.SetBlockScanTargetFunc(func(target types.ScanTargetParam) types.ScanTargetResult {
		if target.ScanTarget == "0x301db155664284b1462e1a10c082a9ff6e2b617f" {
			return types.ScanTargetResult{Exist: true, SourceKey: "test1"}
		}
		return types.ScanTargetResult{Exist: true, SourceKey: "test2"}
	})
	_ = bs.SetTokenMetadataFunc(func(symbol, contractAddr string) *types.SmartContract { return nil })

	start := time.Now()
	res, err := rawScanner.ScanBlockOnce(uint64(testHeight))
	if err != nil {
		t.Fatalf("ScanBlockOnce(%d) error: %v", testHeight, err)
	}
	a, _ := json.Marshal(res)
	fmt.Println(string(a))
	if res == nil {
		t.Fatalf("nil ScanBlockOnce result")
	}
	if res.Height != uint64(testHeight) {
		t.Fatalf("unexpected height: got=%d want=%d", res.Height, testHeight)
	}
	if res.Header == nil {
		t.Fatalf("nil header in result: %+v", res)
	}
	if res.Header.Height != uint64(testHeight) {
		t.Fatalf("unexpected header.height: got=%d want=%d", res.Header.Height, testHeight)
	}
	if res.BlockHash == "" || res.Header.Hash == "" || res.Header.Hash != res.BlockHash {
		t.Fatalf("unexpected block hash/header hash: blockHash=%q headerHash=%q", res.BlockHash, func() string {
			if res.Header == nil {
				return ""
			}
			return res.Header.Hash
		}())
	}

	t.Logf("ScanBlockOnce ok: height=%d latest=%d conf=%d success=%v err=%q txTotal=%d txFailed=%d extracted=%d elapsed=%s",
		res.Height, res.NetworkBlockHeight, res.Header.Confirmations, res.Success, res.ErrorReason,
		res.TxTotal, res.TxFailed, res.ExtractedTxs, time.Since(start))
}

// TestScanBlockOnce 测试“指定高度补扫一次”的能力（不进入持续循环）。
func TestScanBlockOnceToken(t *testing.T) {
	const iniContent = testIniContent

	adapter, err := eth.NewAdapter(iniContent, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}

	// 全命中，确保能产出结果集（若本地节点对应高度无交易也可正常返回）。
	_ = bs.SetBlockScanTargetFunc(func(target types.ScanTargetParam) types.ScanTargetResult {
		if target.ScanTarget == "0x301db155664284b1462e1a10c082a9ff6e2b617f" {
			return types.ScanTargetResult{Exist: true, SourceKey: "test1"}
		}
		return types.ScanTargetResult{Exist: true, SourceKey: "test2"}
	})
	_ = bs.SetTokenMetadataFunc(func(symbol, contractAddr string) *types.SmartContract { return nil })

	start := time.Now()
	res, err := rawScanner.ScanBlockOnce(uint64(testHeightToken))
	if err != nil {
		t.Fatalf("ScanBlockOnce(%d) error: %v", testHeightToken, err)
	}
	a, _ := json.Marshal(res)
	fmt.Println(string(a))
	if res == nil {
		t.Fatalf("nil ScanBlockOnce result")
	}
	if res.Height != uint64(testHeightToken) {
		t.Fatalf("unexpected height: got=%d want=%d", res.Height, testHeightToken)
	}
	if res.Header == nil {
		t.Fatalf("nil header in result: %+v", res)
	}
	if res.Header.Height != uint64(testHeightToken) {
		t.Fatalf("unexpected header.height: got=%d want=%d", res.Header.Height, testHeightToken)
	}
	if res.BlockHash == "" || res.Header.Hash == "" || res.Header.Hash != res.BlockHash {
		t.Fatalf("unexpected block hash/header hash: blockHash=%q headerHash=%q", res.BlockHash, func() string {
			if res.Header == nil {
				return ""
			}
			return res.Header.Hash
		}())
	}

	t.Logf("ScanBlockOnce ok: height=%d latest=%d conf=%d success=%v err=%q txTotal=%d txFailed=%d extracted=%d elapsed=%s",
		res.Height, res.NetworkBlockHeight, res.Header.Confirmations, res.Success, res.ErrorReason,
		res.TxTotal, res.TxFailed, res.ExtractedTxs, time.Since(start))
}

func TestRunScanLoopContinuously(t *testing.T) {
	adapter, err := eth.NewAdapter(testIniContent, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()

	// 让扫块器真正运行提取逻辑（可根据本地节点情况调整）。
	_ = rawScanner.SetBlockScanTargetFunc(func(target types.ScanTargetParam) types.ScanTargetResult {
		return types.ScanTargetResult{Exist: true, SourceKey: "test"}
	})
	_ = rawScanner.SetTokenMetadataFunc(func(symbol, contractAddr string) *types.SmartContract { return nil })

	latest := rawScanner.GetGlobalMaxBlockHeight()
	if latest < 2 {
		t.Skipf("latest block height too small: %d", latest)
	}

	// confirmations 用于确定安全高度 safeTo=latest-confirmations。
	// 本次观察将确认数设为 6，并使 windowSize=6，保证在追平 latest 后会持续循环覆盖最近 6 个安全块。
	var confirmations uint64 = 6
	var windowSize uint64 = 0
	startHeight := testHeight

	go func() {
		if err := rawScanner.RunScanLoop(uint64(startHeight), confirmations, windowSize, 5*time.Second, func(res *types.BlockScanResult) {
			if res == nil {
				fmt.Printf("[RunScanLoop] nil result\n")
				return
			}
			time.Sleep(100 * time.Millisecond)
			latestMax := rawScanner.GetGlobalMaxBlockHeight()
			headerHash := ""
			prevHash := ""
			if res.Header != nil {
				headerHash = res.Header.Hash
				prevHash = res.Header.Previousblockhash
			}
			fmt.Printf("[RunScanLoop] scanHeight=%d latestMax=%d hash=%s header=%s prev=%s success=%v err=%q txTotal=%d txFailed=%d extracted=%d\n",
				res.Height, latestMax, res.BlockHash, headerHash, prevHash, res.Success, res.ErrorReason,
				res.TxTotal, res.TxFailed, res.ExtractedTxs)
		}); err != nil {
			fmt.Printf("[RunScanLoop] error: %v\n", err)
		}
	}()

	time.Sleep(5 * time.Second)
	t.Log("重置高度----")
	rawScanner.ResetScanHeight(testHeight)

	t.Logf("RunScanLoop started: latest=%d startHeight=%d confirmations=%d windowSize=%d, running ~10 minutes...",
		latest, startHeight, confirmations, windowSize)
	time.Sleep(10 * time.Minute)
}
