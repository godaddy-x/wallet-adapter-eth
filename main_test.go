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
	adaptscanner "github.com/godaddy-x/wallet-adapter/scanner"
	"github.com/godaddy-x/wallet-adapter/types"
	"github.com/tidwall/gjson"
)

// testConfigJSON 为本仓库本地节点集成测试的通用 JSON 配置。
// 如需切换节点端口/数据目录，只需修改此处。
const testConfigJSON = `{
  "serverAPI": "http://43.135.98.105:8547",
  "broadcastAPI": "http://43.135.98.105:8547",
  "fixGasLimit": "",
  "dataDir": "E://test/",
  "fixGasPrice": "",
  "offsetsGasPrice": "0",
  "nonceComputeMode": "0",
  "useQNSingleFlightRPC": "0",
  "detectUnknownContracts": "0",
  "moralisAPIKey": "",
  "moralisAPIChain": "eth",
  "useMoralisAPIParseBlock": "0"
}`

const testHeight = 461929
const testHeightToken = 426525

// TestStartBlockScanner 演示如何在业务侧完整初始化适配器和扫块器：
// - 通过 eth.NewAdapter 创建适配器（内部构造 EthBlockScanner）；
// - 设置 ScanTargetFunc：告诉扫块器“哪些地址/合约是我关心的扫描目标”（本用例不关心交易提取，因此默认都不匹配）；
// - 实际连本地节点：获取 latest 区块头并执行一次 ScanBlock，验证扫块流程可跑通。
func TestStartBlockScanner(t *testing.T) {
	// 复用 main.go 中的 demo 配置（本地节点地址可按需要调整）
	const configJSON = testConfigJSON

	// 创建适配器（内部会构造 EthBlockScanner，但此时尚未设置 ScanTargetFunc）
	adapter, err := eth.NewAdapter(configJSON, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}

	// 断言 BlockScanner 类型为我们实现的 EthBlockScanner，以便访问嵌入的 Base 字段
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}

	// 1）设置 ScanTargetFunc：本用例不关心交易提取，因此全部返回 nil
	_ = bs.SetBlockScanTargetFunc(func(target *types.ScanTargetParam) error {
		return nil
	})

	// 2）连节点取 latest 区块并扫一次（同步返回结果集）
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
// 该用例依赖本地节点可用（使用与 TestStartBlockScanner 相同的 JSON 配置）。
func TestScanBlockWithResultFlow(t *testing.T) {
	const configJSON = testConfigJSON

	adapter, err := eth.NewAdapter(configJSON, "ETH", "Ethereum", 18)
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
	_ = bs.SetBlockScanTargetFunc(func(target *types.ScanTargetParam) error {
		if target == nil {
			return nil
		}
		for scanTarget := range target.ScanTarget {
			switch target.ScanTargetType {
			case types.ScanTargetTypeContractAddress:
				target.ScanTarget[scanTarget] = &types.Coin{
					Symbol:     "ETH",
					IsContract: true,
					Contract: types.SmartContract{
						Address:  strings.ToLower(scanTarget),
						Decimals: 18,
					},
				}
			default:
				target.ScanTarget[scanTarget] = "test"
			}
		}
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
	for _, item := range res.ExtractData {
		if item == nil {
			continue
		}
		extractCount += uint64(len(item.Data))
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
	const configJSON = testConfigJSON

	adapter, err := eth.NewAdapter(configJSON, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}

	// 全命中，确保能产出结果集（注意：Verify 仍会受"tx 是否成功/确认数"影响）
	_ = bs.SetBlockScanTargetFunc(func(target *types.ScanTargetParam) error {
		if target == nil {
			return nil
		}
		for scanTarget := range target.ScanTarget {
			switch target.ScanTargetType {
			case types.ScanTargetTypeContractAddress:
				target.ScanTarget[scanTarget] = &types.Coin{
					Symbol:     "ETH",
					IsContract: true,
					Contract: types.SmartContract{
						Address:  strings.ToLower(scanTarget),
						Decimals: 18,
					},
				}
			default:
				target.ScanTarget[scanTarget] = "test"
			}
		}
		return nil
	})

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
	const configJSON = testConfigJSON

	adapter, err := eth.NewAdapter(configJSON, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}
	_ = bs.SetBlockScanTargetFunc(func(target *types.ScanTargetParam) error {
		if target == nil {
			return nil
		}
		for scanTarget := range target.ScanTarget {
			switch target.ScanTargetType {
			case types.ScanTargetTypeContractAddress:
				target.ScanTarget[scanTarget] = &types.Coin{
					Symbol:     "ETH",
					IsContract: true,
					Contract: types.SmartContract{
						Address:  strings.ToLower(scanTarget),
						Decimals: 18,
					},
				}
			default:
				target.ScanTarget[scanTarget] = "test"
			}
		}
		return nil
	})

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
	for _, item := range chain.ExtractData {
		if item == nil {
			continue
		}
		for _, d := range item.Data {
			if d == nil || d.Transaction == nil {
				continue
			}
			tx := d.Transaction
			if tx.Coin.IsContract {
				continue
			}
			// 主币 value=0 的交易不会产生交易单，这里拿到的应该都有 amount
			if len(tx.FromAddr) > 0 {
				from = strings.ToLower(tx.FromAddr[0])
				if len(tx.FromAmt) > 0 {
					amountStr = tx.FromAmt[0]
				}
			}
			if len(tx.ToAddr) > 0 {
				to = strings.ToLower(tx.ToAddr[0])
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
			{ContractAddr: "", From: from, To: to, Amount: amountStr, Decimals: 0, OutputIndex: -1},
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
	const configJSON = testConfigJSON

	adapter, err := eth.NewAdapter(configJSON, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}

	// 全命中，确保能产出结果集（若本地节点对应高度无交易也可正常返回）。
	_ = bs.SetBlockScanTargetFunc(func(target *types.ScanTargetParam) error {
		if target == nil {
			return nil
		}
		for scanTarget := range target.ScanTarget {
			if target.ScanTargetType == types.ScanTargetTypeContractAddress {
				target.ScanTarget[scanTarget] = &types.Coin{
					Symbol:     "ETH",
					IsContract: true,
					Contract: types.SmartContract{
						Address:  strings.ToLower(scanTarget),
						Decimals: 18,
					},
				}
				continue
			}
			if strings.EqualFold(scanTarget, "0x301db155664284b1462e1a10c082a9ff6e2b617f") {
				target.ScanTarget[scanTarget] = "test1"
			} else {
				target.ScanTarget[scanTarget] = "test2"
			}
		}
		return nil
	})

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
	const configJSON = testConfigJSON

	adapter, err := eth.NewAdapter(configJSON, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()
	bs, ok := rawScanner.(*ethscanner.EthBlockScanner)
	if !ok {
		t.Fatalf("unexpected BlockScanner type: %T", rawScanner)
	}

	// 全命中，确保能产出结果集（若本地节点对应高度无交易也可正常返回）。
	_ = bs.SetBlockScanTargetFunc(func(target *types.ScanTargetParam) error {
		if target == nil {
			return nil
		}
		for scanTarget := range target.ScanTarget {
			if target.ScanTargetType == types.ScanTargetTypeContractAddress {
				target.ScanTarget[scanTarget] = &types.Coin{
					Symbol:     "ETH",
					IsContract: true,
					Contract: types.SmartContract{
						Address:  strings.ToLower(scanTarget),
						Decimals: 18,
					},
				}
				continue
			}
			target.ScanTarget[scanTarget] = "test1"
		}
		return nil
	})

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
	adapter, err := eth.NewAdapter(testConfigJSON, "ETH", "Ethereum", 18)
	if err != nil {
		t.Fatalf("NewAdapter error: %v", err)
	}
	rawScanner := adapter.GetBlockScanner()

	// 让扫块器真正运行提取逻辑（可根据本地节点情况调整）。
	_ = rawScanner.SetBlockScanTargetFunc(func(target *types.ScanTargetParam) error {
		if target == nil {
			return nil
		}
		for scanTarget := range target.ScanTarget {
			if target.ScanTargetType == types.ScanTargetTypeContractAddress {
				target.ScanTarget[scanTarget] = &types.Coin{
					Symbol:     "ETH",
					IsContract: true,
					Contract: types.SmartContract{
						Address:  strings.ToLower(scanTarget),
						Decimals: 18,
					},
				}
			} else {
				target.ScanTarget[scanTarget] = "test"
			}
		}
		return nil
	})

	latest := rawScanner.GetGlobalMaxBlockHeight()
	if latest < 2 {
		t.Skipf("latest block height too small: %d", latest)
	}

	// confirmations 仅用于计算 BlockHeader.Confirmations 字段供业务层参考
	var confirmations uint64 = 6
	startHeight := testHeight

	go func() {
		if err := rawScanner.RunScanLoop(adaptscanner.ScanLoopParams{
			StartHeight:   uint64(startHeight),
			Confirmations: confirmations,
			Interval:      5 * time.Second,
			HandleBlock: func(res *types.BlockScanResult) {
				if res == nil {
					fmt.Printf("[RunScanLoop] nil result\n")
					return
				}
				time.Sleep(100 * time.Millisecond)
				if res.Once {
					fmt.Printf("[RunScanLoop] once -------------- %d - %d\n", res.Height, res.NetworkBlockHeight)
				}
				latestMax := rawScanner.GetGlobalMaxBlockHeight()
				headerHash := ""
				prevHash := ""
				if res.Header != nil {
					headerHash = res.Header.Hash
					prevHash = res.Header.Previousblockhash
				}
				fmt.Printf("[RunScanLoop] scanHeight=%d latestMax=%d hash=%s header=%s prev=%s success=%v err=%q txTotal=%d txFailed=%d extracted=%d once=%v\n",
					res.Height, latestMax, res.BlockHash, headerHash, prevHash, res.Success, res.ErrorReason,
					res.TxTotal, res.TxFailed, res.ExtractedTxs, res.Once)
			},
		}); err != nil {
			fmt.Printf("[RunScanLoop] error: %v\n", err)
		}
	}()

	time.Sleep(2 * time.Second)

	// 获取当前 latest 用于调试
	latestBefore := rawScanner.GetGlobalMaxBlockHeight()
	t.Logf("Before ScanBlockPrioritize: latest=%d", latestBefore)

	// 使用合理的高度范围：在 safeTo = latest - 6 范围内选择
	// 假设 latest ~ 83156+10=83166, safeTo = 83160
	// 选择高度 83150（在 startHeight 83156 附近，且在 safeTo 范围内）
	prioritizeHeights := []uint64{1, 100, 1333}
	if err := rawScanner.ScanBlockPrioritize(prioritizeHeights); err != nil {
		t.Fatalf("ScanBlockPrioritize error: %v", err)
		return
	}
	t.Logf("ScanBlockPrioritize called with heights: %v", prioritizeHeights)

	t.Logf("RunScanLoop started: latest=%d startHeight=%d confirmations=%d running ~10 minutes...",
		latest, startHeight, confirmations)
	time.Sleep(10 * time.Minute)
}
