package scanner

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/godaddy-x/wallet-adapter-eth/internal/manager"
	"github.com/godaddy-x/wallet-adapter-eth/internal/util"
	adaptscanner "github.com/godaddy-x/wallet-adapter/scanner"
	"github.com/godaddy-x/wallet-adapter/types"
)

func splitAddrAmount(s string) (addr string, amount string) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) == 2 {
		return strings.ToLower(parts[0]), parts[1]
	}
	return strings.ToLower(s), ""
}

// EthBlockScanner 以太坊区块扫描器，实现 wallet-adapter 的 BlockScanner 接口。
// - 嵌入 Base，复用观察者管理、扫描任务调度、BlockchainDAI 持久化接口；
// - 通过 WalletManager.RPC 查询最新区块高度与区块详情；
// - 针对新区块构造 BlockHeader 并通过 NewBlockNotify 广播给观察者；
// - 提供 GetBlockchainSyncStatus / GetBalanceByAddress 等常用能力；
// - 提供 ExtractTransactionAndReceiptData 按 txid 精准提取主币与 ERC20 交易单，并通过观察者回调推送。
type EthBlockScanner struct {
	*adaptscanner.Base
	// wm 为链上 RPC 与元数据能力的入口（如 SymbolDecimal、ERC20Metadata）
	wm *manager.WalletManager

	// TxExtractConcurrency 控制单个区块内并行提取交易的并发度。
	// 建议根据节点吞吐与限流策略调整（如 5/10/20）。<=0 时使用默认值。
	TxExtractConcurrency int

	// tokenDecimalsCache 按合约地址缓存 ERC20 精度，避免每个事件都发 ERC20Metadata RPC
	// 约定：>0 为有效精度；=0 表示“查询失败/非标准 ERC20”，调用方需跳过该事件，绝不猜测默认 18。
	tokenDecimalsCache   map[string]int32
	tokenDecimalsCacheMu sync.RWMutex
}

// VerifyTransactionByTxID 入账前按 txid 二次复核链上结果并返回可入账结果集。
// 复核点：
// - 交易必须已上链（blockNumber/blockHash 非空）；
// - 回执 status 必须成功；
// - confirmations >= minConfirmations；
// - 使用与扫块一致的提取逻辑 ExtractTransactionAndReceiptData 生成结果集，保证口径一致。
func (bs *EthBlockScanner) VerifyTransactionByTxID(txid string, scanTargetFunc adaptscanner.BlockScanTargetFunc, minConfirmations uint64) (*types.TxVerifyResult, error) {
	res := &types.TxVerifyResult{
		Symbol:           "",
		TxID:             txid,
		Verified:         false,
		Reason:           "",
		BlockHeight:      0,
		BlockHash:        "",
		Confirmations:    0,
		Status:           "",
		ExtractData:      make(map[string][]*types.TxExtractData),
		ContractReceipts: make(map[string]*types.SmartContractReceipt),
	}
	if bs.wm != nil && bs.wm.Config != nil {
		res.Symbol = bs.wm.Config.Symbol
	}
	if bs.wm == nil || bs.wm.Client == nil {
		return res, fmt.Errorf("wallet manager or rpc client is nil")
	}
	if bs.wm.Config == nil {
		return res, fmt.Errorf("wallet manager config is nil")
	}
	if scanTargetFunc == nil {
		return res, fmt.Errorf("scan target func is nil")
	}

	// 1) 查询交易，必须已上链
	txRes, err := bs.wm.Client.Call("eth_getTransactionByHash", []interface{}{txid})
	if err != nil {
		return res, err
	}
	if !txRes.Exists() || txRes.Type == 0 {
		res.Reason = "tx not found"
		return res, nil
	}
	blockNumberHex := txRes.Get("blockNumber").String()
	blockHash := txRes.Get("blockHash").String()
	if blockNumberHex == "" || blockHash == "" {
		res.Reason = "tx pending or not mined"
		return res, nil
	}
	if h, err := hexutil.DecodeUint64(blockNumberHex); err == nil {
		res.BlockHeight = h
	}
	res.BlockHash = blockHash

	// 2) 查询回执，必须成功
	rcRes, err := bs.wm.Client.Call("eth_getTransactionReceipt", []interface{}{txid})
	if err != nil {
		return res, err
	}
	if !rcRes.Exists() || rcRes.Type == 0 {
		res.Reason = "receipt not found"
		return res, nil
	}
	status := types.TxStatusFail
	if statusHex := rcRes.Get("status").String(); statusHex != "" {
		if st, err := hexutil.DecodeUint64(statusHex); err == nil && st == 1 {
			status = types.TxStatusSuccess
		}
	}
	res.Status = status
	if status == types.TxStatusFail {
		res.Reason = "tx failed"
		return res, nil
	}

	// 3) 确认数校验
	latest := bs.GetGlobalMaxBlockHeight()
	if latest == 0 || res.BlockHeight == 0 || latest < res.BlockHeight {
		res.Reason = "cannot compute confirmations"
		return res, nil
	}
	res.Confirmations = latest - res.BlockHeight + 1
	if res.Confirmations < minConfirmations {
		res.Reason = "not enough confirmations"
		return res, nil
	}

	// 4) 使用同一套提取逻辑生成结果集（口径一致）
	extractData, contractReceipts, extractErr := bs.ExtractTransactionAndReceiptData(txid, scanTargetFunc)
	if extractErr != nil {
		res.Reason = "extract failed: " + extractErr.Error()
		return res, nil
	}
	res.ExtractData = extractData
	res.ContractReceipts = contractReceipts
	res.Verified = true
	return res, nil
}

// VerifyTransactionMatch 在 VerifyTransactionByTxID 基础上，将链上提取到的主币/代币转账与 expected 做严格比对。
// 比对范围：txid、blockHeight、blockHash、以及 transfers 的 from/to/amount（主币或指定合约代币）。
func (bs *EthBlockScanner) VerifyTransactionMatch(txid string, expected *types.TxVerifyExpected, scanTargetFunc adaptscanner.BlockScanTargetFunc, minConfirmations uint64) (*types.TxVerifyMatchResult, error) {
	out := &types.TxVerifyMatchResult{
		TxID:       txid,
		Verified:   false,
		Reason:     "",
		Mismatches: make([]string, 0),
		Chain:      nil,
	}
	if expected == nil {
		out.Reason = "expected is nil"
		return out, nil
	}
	// 严格模式：expected 必须提供关键字段，否则无法证明“一致”
	if expected.TxID == "" || expected.BlockHash == "" || expected.Height == 0 {
		if expected.TxID == "" {
			out.Mismatches = append(out.Mismatches, "expected.txid is empty")
		}
		if expected.BlockHash == "" {
			out.Mismatches = append(out.Mismatches, "expected.blockHash is empty")
		}
		if expected.Height == 0 {
			out.Mismatches = append(out.Mismatches, "expected.height is 0")
		}
		out.Reason = "invalid expected"
		return out, nil
	}
	if len(expected.Transfers) == 0 {
		out.Mismatches = append(out.Mismatches, "expected.transfers is empty")
		out.Reason = "invalid expected"
		return out, nil
	}

	chain, err := bs.VerifyTransactionByTxID(txid, scanTargetFunc, minConfirmations)
	if err != nil {
		return nil, err
	}
	out.Chain = chain
	if chain == nil || !chain.Verified {
		out.Reason = "chain verify failed: " + func() string {
			if chain == nil {
				return "nil chain result"
			}
			if chain.Reason != "" {
				return chain.Reason
			}
			return "unknown"
		}()
		return out, nil
	}

	// 1) txid / block 必须严格对齐
	if !strings.EqualFold(expected.TxID, txid) {
		out.Mismatches = append(out.Mismatches, "txid mismatch")
	}
	if expected.Height != chain.BlockHeight {
		out.Mismatches = append(out.Mismatches, fmt.Sprintf("height mismatch: expected=%d actual=%d", expected.Height, chain.BlockHeight))
	}
	if !strings.EqualFold(expected.BlockHash, chain.BlockHash) {
		out.Mismatches = append(out.Mismatches, "blockHash mismatch")
	}

	// helper: build raw amount from decimal string
	toRaw := func(amount string, decimals int32) (*big.Int, error) {
		return util.StringToBigInt(amount, decimals)
	}

	// 3) 校验 expected transfers
	for _, tr := range expected.Transfers {
		if tr == nil {
			out.Mismatches = append(out.Mismatches, "expected.transfer is nil")
			continue
		}
		expFrom := strings.ToLower(tr.From)
		expTo := strings.ToLower(tr.To)
		contract := strings.ToLower(tr.ContractAddr)
		if expFrom == "" || expTo == "" || strings.TrimSpace(tr.Amount) == "" {
			out.Mismatches = append(out.Mismatches, "expected transfer missing from/to/amount")
			continue
		}

		// 主币：contractAddr 为空，直接用链上 tx 的 from/to/value 校验
		if contract == "" {
			// 用 VerifyTransactionByTxID 里已经校验成功的 tx，再拉一次 tx 取 value/from/to（避免依赖 extractData 格式化差异）
			txRes, err := bs.wm.Client.Call("eth_getTransactionByHash", []interface{}{txid})
			if err != nil {
				return nil, err
			}
			from := strings.ToLower(txRes.Get("from").String())
			to := strings.ToLower(txRes.Get("to").String())
			if expFrom != from {
				out.Mismatches = append(out.Mismatches, fmt.Sprintf("native from mismatch: expected=%s actual=%s", expFrom, from))
				continue
			}
			if expTo != to {
				out.Mismatches = append(out.Mismatches, fmt.Sprintf("native to mismatch: expected=%s actual=%s", expTo, to))
				continue
			}
			valueHex := txRes.Get("value").String()
			val := big.NewInt(0)
			if valueHex != "" && valueHex != "0x" {
				if v, err := hexutil.DecodeBig(valueHex); err == nil {
					val = v
				}
			}
			expRaw, err := toRaw(tr.Amount, bs.wm.SymbolDecimal())
			if err != nil || expRaw.Cmp(val) != 0 {
				out.Mismatches = append(out.Mismatches, "native amount mismatch")
			}
			continue
		}

		// 代币：严格模式必须提供 logIndex，用于唯一定位同一 tx 内的 Transfer 事件
		if tr.LogIndex < 0 {
			out.Mismatches = append(out.Mismatches, "token logIndex required for strict match")
			continue
		}
		expDecimals := int32(tr.Decimals)
		if expDecimals <= 0 {
			out.Mismatches = append(out.Mismatches, "token decimals missing")
			continue
		}
		expRaw, err := toRaw(tr.Amount, expDecimals)
		if err != nil {
			out.Mismatches = append(out.Mismatches, "token amount parse error")
			continue
		}
		k := txid + ":" + contract + ":" + strconv.FormatInt(tr.LogIndex, 10)
		r, ok := chain.ContractReceipts[k]
		if !ok || r == nil {
			out.Mismatches = append(out.Mismatches, "token receipt not found by logIndex")
			continue
		}
		if strings.ToLower(r.From) != expFrom {
			out.Mismatches = append(out.Mismatches, "token from mismatch")
			continue
		}
		if strings.ToLower(r.To) != expTo {
			out.Mismatches = append(out.Mismatches, "token to mismatch")
			continue
		}
		actRaw, aErr := toRaw(r.Value, expDecimals)
		if aErr != nil || actRaw.Cmp(expRaw) != 0 {
			out.Mismatches = append(out.Mismatches, "token amount mismatch")
			continue
		}
	}

	if len(out.Mismatches) == 0 {
		out.Verified = true
		out.Reason = ""
		return out, nil
	}
	out.Verified = false
	out.Reason = "mismatch"
	return out, nil
}

// ScanBlockWithResult 按高度扫描并返回结果摘要，供外部系统推进游标与重试。
// 当前实现以 ScanBlock 的返回值为准填充 Success/ErrorReason；更细粒度统计可按需扩展。
func (bs *EthBlockScanner) ScanBlockWithResult(height uint64) (*types.BlockScanResult, error) {
	res := &types.BlockScanResult{
		Symbol:       "",
		Height:       height,
		Success:      false,
		FailedTxIDs:  make([]string, 0),
		TxTotal:      0,
		TxFailed:     0,
		ExtractedTxs: 0,
		ExtractData:  make(map[string][]*types.TxExtractData),
		ContractReceipts: make(map[string]*types.SmartContractReceipt),
	}
	if bs.wm != nil && bs.wm.Config != nil {
		res.Symbol = bs.wm.Config.Symbol
	}
	if bs.wm == nil || bs.wm.Client == nil {
		res.ErrorReason = "wallet manager or rpc client is nil"
		return res, fmt.Errorf("%s", res.ErrorReason)
	}
	if bs.wm.Config == nil {
		res.ErrorReason = "wallet manager config is nil"
		return res, fmt.Errorf("%s", res.ErrorReason)
	}

	// 1) 拉取完整区块（与 ScanBlock 一致）
	tag := fmt.Sprintf("0x%x", height)
	result, err := bs.wm.Client.Call("eth_getBlockByNumber", []interface{}{tag, true})
	if err != nil {
		res.ErrorReason = err.Error()
		return res, err
	}
	numberHex := result.Get("number").String()
	hash := result.Get("hash").String()
	if numberHex == "" || hash == "" {
		res.ErrorReason = fmt.Sprintf("block not found for height %d", height)
		return res, fmt.Errorf("%s", res.ErrorReason)
	}
	res.BlockHash = hash

	blockHeight, err := hexutil.DecodeUint64(numberHex)
	if err != nil {
		res.ErrorReason = err.Error()
		return res, err
	}
	parentHash := result.Get("parentHash").String()
	timeHex := result.Get("timestamp").String()
	var timestamp uint64
	if timeHex != "" {
		if ts, err := hexutil.DecodeUint64(timeHex); err == nil {
			timestamp = ts
		}
	}

	header := &types.BlockHeader{
		Hash:              hash,
		Previousblockhash: parentHash,
		Height:            blockHeight,
		Time:              timestamp,
		Symbol:            bs.wm.Config.Symbol,
	}
	res.Header = header

	// 2) 逐笔提取交易（仅在设置 ScanTargetFunc 时进行；统计失败与提取数量，供外部观测）
	// 注意：ScanBlockWithResult 为“同步返回结果集”模式，不在此处做异步通知推送。
	if bs.ScanTargetFunc != nil {
		txs := result.Get("transactions").Array()
		res.TxTotal = uint64(len(txs))

		// worker pool 并行提取交易（块内并行、块间仍串行），提升吞吐并保持外部游标推进语义简单
		type txExtractOut struct {
			txHash          string
			extractData     map[string][]*types.TxExtractData
			contractReceipts map[string]*types.SmartContractReceipt
			err             error
		}

		txHashes := make([]string, 0, len(txs))
		for _, txNode := range txs {
			h := txNode.Get("hash").String()
			if h != "" {
				txHashes = append(txHashes, h)
			}
		}

		workers := bs.TxExtractConcurrency
		if workers <= 0 {
			workers = 10
		}
		if workers > len(txHashes) && len(txHashes) > 0 {
			workers = len(txHashes)
		}
		if workers == 0 {
			workers = 1
		}

		jobs := make(chan string, workers)
		outs := make(chan txExtractOut, workers)

		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				for txHash := range jobs {
					ed, cr, e := bs.ExtractTransactionAndReceiptData(txHash, bs.ScanTargetFunc)
					outs <- txExtractOut{
						txHash:           txHash,
						extractData:      ed,
						contractReceipts: cr,
						err:              e,
					}
				}
			}()
		}

		go func() {
			for _, h := range txHashes {
				jobs <- h
			}
			close(jobs)
			wg.Wait()
			close(outs)
		}()

		// 单线程合并结果，避免并发写 map
		for out := range outs {
			if out.err != nil {
				res.TxFailed++
				if len(res.FailedTxIDs) < 20 {
					res.FailedTxIDs = append(res.FailedTxIDs, out.txHash)
				}
				continue
			}

			if len(out.extractData) > 0 {
				for k, list := range out.extractData {
					res.ExtractData[k] = append(res.ExtractData[k], list...)
					res.ExtractedTxs += uint64(len(list))
				}
			}
			if len(out.contractReceipts) > 0 {
				for k, v := range out.contractReceipts {
					res.ContractReceipts[k] = v
				}
				res.ExtractedTxs += uint64(len(out.contractReceipts))
			}
		}
	} else {
		// 未设置扫描目标时，不提取交易，只返回区块扫描“可用”的结果（TxTotal=0）
	}

	// Success 语义：本高度区块可成功获取并完成扫描流程；若存在 tx 提取失败，则 Success=false 并给出摘要原因
	if res.TxFailed == 0 {
		res.Success = true
		return res, nil
	}
	res.Success = false
	res.ErrorReason = fmt.Sprintf("block scanned with %d tx extraction failures", res.TxFailed)
	return res, nil
}

// NewBlockScanner 创建以太坊区块扫描器，并设置默认扫描任务（周期由 Base.PeriodOfTask 控制，默认 5s 一次）。
// 具体扫描逻辑 scanBlockTask 会从当前已扫高度向最新链高度推进，逐块调用 ScanBlock。
// TokenMetadataFunc 现由外部在 Base 上统一注入（与 ScanTargetFunc 一致），此处仅依赖其存在而不再单独持有。
func NewBlockScanner(wm *manager.WalletManager) *EthBlockScanner {
	bs := &EthBlockScanner{
		Base:               adaptscanner.NewBlockScannerBase(),
		wm:                 wm,
		TxExtractConcurrency: 10,
		tokenDecimalsCache: make(map[string]int32),
	}
	// 设置定时任务为按高度顺序扫描新区块
	bs.SetTask(bs.scanBlockTask)
	return bs
}

// SetTxExtractConcurrency 动态设置单区块内并行提取交易的并发度。
// 建议取值：1~100；过高可能触发节点限流或导致 RPC 超时。
func (bs *EthBlockScanner) SetTxExtractConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	if n > 100 {
		n = 100
	}
	bs.TxExtractConcurrency = n
}

// getTokenDecimals 返回合约代币精度，带缓存。
// 成功返回 (decimals, true)，查询不到 / 非 ERC20 / RPC 失败时返回 (0, false)，由调用方决定是否跳过该事件。
// 设计原则：宁可“不入账”未知代币，也不能用错误精度（例如把 6 位小数按 18 位展示为 0.000001），因此绝不在此处猜测默认 18。
func (bs *EthBlockScanner) getTokenDecimals(contractAddr string) (int32, bool) {
	if contractAddr == "" || bs.wm == nil {
		return 0, false
	}
	bs.tokenDecimalsCacheMu.RLock()
	d, ok := bs.tokenDecimalsCache[contractAddr]
	bs.tokenDecimalsCacheMu.RUnlock()
	if ok {
		// 仅当缓存中为有效精度时使用；0 视为“已知失败”，直接让调用方跳过
		if d > 0 {
			return d, true
		}
		return 0, false
	}

	// 1）优先调用 Base 上由上层业务注入的 TokenMetadataFunc，允许业务系统按自身资产配置提供精度信息。
	// 约定：返回的 SmartContract 非空且 Decimals>0 表示业务侧已配置该合约的有效精度。
	if bs.TokenMetadataFunc != nil && bs.wm.Config != nil {
		if sc := bs.TokenMetadataFunc(bs.wm.Config.Symbol, contractAddr); sc != nil && sc.Decimals > 0 {
			decimals := int32(sc.Decimals)
			bs.tokenDecimalsCacheMu.Lock()
			if _, exists := bs.tokenDecimalsCache[contractAddr]; !exists {
				bs.tokenDecimalsCache[contractAddr] = decimals
			}
			bs.tokenDecimalsCacheMu.Unlock()
			return decimals, true
		}
	}

	// 2）若业务侧未能给出有效精度，则查询链上 ERC20 metadata（包含 name/symbol/decimals，当前仅关心 decimals）
	_, _, dec, err := bs.wm.ERC20Metadata(contractAddr)
	if err != nil || dec == 0 {
		// 记录一次错误日志，便于排查问题；不冒然假定为 18，避免金额严重偏差
		fmt.Printf("[EthBlockScanner] getTokenDecimals failed for contract %s: %v, decimals=%d\n", contractAddr, err, dec)
		// 缓存一个无效标记，避免后续重复打日志 / 重复查询
		bs.tokenDecimalsCacheMu.Lock()
		if _, exists := bs.tokenDecimalsCache[contractAddr]; !exists {
			bs.tokenDecimalsCache[contractAddr] = 0
		}
		bs.tokenDecimalsCacheMu.Unlock()
		return 0, false
	}

	decimals := int32(dec)
	bs.tokenDecimalsCacheMu.Lock()
	if _, exists := bs.tokenDecimalsCache[contractAddr]; !exists {
		bs.tokenDecimalsCache[contractAddr] = decimals
	}
	bs.tokenDecimalsCacheMu.Unlock()
	return decimals, true
}

// scanBlockTask 周期性扫描新区块：从已扫高度+1 扫描到当前链上最新高度。
func (bs *EthBlockScanner) scanBlockTask() {
	if bs.wm == nil || bs.wm.Client == nil {
		return
	}
	current := bs.GetScannedBlockHeight()
	latest := bs.GetGlobalMaxBlockHeight()
	// latest==0 代表节点尚未返回合法高度，或 RPC 错误时的保护；latest<=current 则表示已追平链上高度
	if latest == 0 || latest <= current {
		return
	}
	for h := current + 1; h <= latest; h++ {
		_ = bs.ScanBlock(h)
	}
}

// SetRescanBlockHeight 重置区块链扫描高度：将扫描进度设置为指定高度-1，下一轮任务从该高度开始重新扫描。
// 若设置了 BlockchainDAI，则会写入持久化进度；否则仅影响内存中的扫描进度（通过 GetScannedBlockHeight 读取）。
func (bs *EthBlockScanner) SetRescanBlockHeight(height uint64) error {
	if height == 0 {
		return fmt.Errorf("block height to rescan must be greater than 0")
	}
	// 将已扫高度回退到 height-1：下一轮任务从 height 开始重新扫描
	target := height - 1

	// 若外部配置了 BlockchainDAI，则同步更新当前区块头
	if bs.BlockchainDAI != nil && bs.wm != nil && bs.wm.Config != nil {
		header := &types.BlockHeader{
			Hash:   "",
			Height: target,
			Symbol: bs.wm.Config.Symbol,
		}
		if err := bs.BlockchainDAI.SaveCurrentBlockHead(header); err != nil {
			return err
		}
	}
	return nil
}

// ScanBlock 按高度扫描单个区块：拉取完整区块（含交易列表），广播新区块通知，并逐笔解析主币/Token 交易后通过观察者推送。
func (bs *EthBlockScanner) ScanBlock(height uint64) error {
	if bs.wm == nil || bs.wm.Client == nil {
		return fmt.Errorf("wallet manager or rpc client is nil")
	}
	if bs.wm.Config == nil {
		return fmt.Errorf("wallet manager config is nil")
	}
	tag := fmt.Sprintf("0x%x", height)
	// 拉取完整区块（true = 返回完整交易对象，便于拿 hash 逐笔提取）
	result, err := bs.wm.Client.Call("eth_getBlockByNumber", []interface{}{tag, true})
	if err != nil {
		return err
	}
	numberHex := result.Get("number").String()
	hash := result.Get("hash").String()
	// 节点未同步到该高度或区块不存在时返回 null，避免误解析（与 GetCurrentBlockHeader 行为对齐）
	if numberHex == "" || hash == "" {
		return fmt.Errorf("block not found for height %d", height)
	}
	parentHash := result.Get("parentHash").String()
	timeHex := result.Get("timestamp").String()

	blockHeight, err := hexutil.DecodeUint64(numberHex)
	if err != nil {
		return err
	}
	var timestamp uint64
	if timeHex != "" {
		if ts, err := hexutil.DecodeUint64(timeHex); err == nil {
			timestamp = ts
		}
	}

	header := &types.BlockHeader{
		Hash:              hash,
		Previousblockhash: parentHash,
		Height:            blockHeight,
		Time:              timestamp,
		Symbol:            bs.wm.Config.Symbol,
	}

	// 1. 将新区块通知观察者
	_ = bs.NewBlockNotify(header)

	// 2. 若设置了 ScanTargetFunc，则逐笔解析区块内交易并推送给观察者
	if bs.ScanTargetFunc != nil {
		txs := result.Get("transactions").Array()
		// 无交易时 transations 可能为 null，Array() 返回空切片，安全
		for _, txNode := range txs {
			txHash := txNode.Get("hash").String()
			if txHash == "" {
				continue
			}
			extractData, contractReceipts, extractErr := bs.ExtractTransactionAndReceiptData(txHash, bs.ScanTargetFunc)
			if extractErr != nil {
				if bs.BlockchainDAI != nil {
					_ = bs.BlockchainDAI.SaveUnscanRecord(types.NewUnscanRecord(blockHeight, txHash, extractErr.Error(), bs.wm.Config.Symbol))
				}
				continue
			}
			// 仅当有提取结果时推送，避免空 map 无谓调用观察者
			if len(extractData) > 0 || len(contractReceipts) > 0 {
				bs.newExtractDataNotify(extractData, contractReceipts)
			}
		}
	}

	// 3. 若配置了 BlockchainDAI，则更新当前已扫高度
	if bs.BlockchainDAI != nil {
		_ = bs.BlockchainDAI.SaveCurrentBlockHead(header)
	}
	return nil
}

// newExtractDataNotify 将本块内提取的交易与合约回执推送给所有已注册观察者（对齐 quorum 的 newExtractDataNotify）。
func (bs *EthBlockScanner) newExtractDataNotify(extractData map[string][]*types.TxExtractData, extractContractData map[string]*types.SmartContractReceipt) {
	if extractData == nil && extractContractData == nil {
		return
	}
	bs.Mu.RLock()
	observers := make([]adaptscanner.BlockScanNotificationObject, 0, len(bs.Observers))
	for o := range bs.Observers {
		observers = append(observers, o)
	}
	bs.Mu.RUnlock()

	for _, o := range observers {
		if extractData != nil {
			for sourceKey, list := range extractData {
				for _, data := range list {
					_ = o.BlockExtractDataNotify(sourceKey, data)
				}
			}
		}
		if extractContractData != nil {
			for sourceKey, receipt := range extractContractData {
				_ = o.BlockExtractSmartContractDataNotify(sourceKey, receipt)
			}
		}
	}
}

// GetCurrentBlockHeader 查询链上最新区块头（latest），用于同步状态与初始化扫描进度。
func (bs *EthBlockScanner) GetCurrentBlockHeader() (*types.BlockHeader, error) {
	if bs.wm == nil || bs.wm.Client == nil {
		return nil, fmt.Errorf("wallet manager or rpc client is nil")
	}
	if bs.wm.Config == nil {
		return nil, fmt.Errorf("wallet manager config is nil")
	}
	// latest=false：仅需要区块头信息，不取交易详情
	result, err := bs.wm.Client.Call("eth_getBlockByNumber", []interface{}{"latest", false})
	if err != nil {
		return nil, err
	}
	numberHex := result.Get("number").String()
	hash := result.Get("hash").String()
	// 节点未同步到 latest 时 number/hash 可能为空，此时明确返回错误而非继续解析
	if numberHex == "" || hash == "" {
		return nil, fmt.Errorf("block not found for latest")
	}
	parentHash := result.Get("parentHash").String()
	timeHex := result.Get("timestamp").String()

	height, err := hexutil.DecodeUint64(numberHex)
	if err != nil {
		return nil, err
	}
	var timestamp uint64
	if timeHex != "" {
		if ts, err := hexutil.DecodeUint64(timeHex); err == nil {
			timestamp = ts
		}
	}

	header := &types.BlockHeader{
		Hash:              hash,
		Previousblockhash: parentHash,
		Height:            height,
		Time:              timestamp,
		Symbol:            bs.wm.Config.Symbol,
	}
	return header, nil
}

// GetGlobalMaxBlockHeight 获取链上最新区块高度（network height）。
func (bs *EthBlockScanner) GetGlobalMaxBlockHeight() uint64 {
	if bs.wm == nil || bs.wm.Client == nil {
		return 0
	}
	result, err := bs.wm.Client.Call("eth_blockNumber", nil)
	if err != nil {
		return 0
	}
	h, err := hexutil.DecodeUint64(result.String())
	if err != nil {
		return 0
	}
	return h
}

// GetScannedBlockHeight 获取当前已持久化的扫描高度（若未配置 BlockchainDAI，则返回 0）。
func (bs *EthBlockScanner) GetScannedBlockHeight() uint64 {
	if bs.BlockchainDAI == nil || bs.wm == nil || bs.wm.Config == nil {
		return 0
	}
	header, err := bs.BlockchainDAI.GetCurrentBlockHead(bs.wm.Config.Symbol)
	if err != nil || header == nil {
		return 0
	}
	return header.Height
}

// GetBalanceByAddress 查询一组地址的原生币余额，返回 wallet-adapter 的 Balance 结构列表。
// 用于扫块后快速补充地址余额信息或对账。
func (bs *EthBlockScanner) GetBalanceByAddress(address ...string) ([]*types.Balance, error) {
	if bs.wm == nil {
		return nil, fmt.Errorf("wallet manager is nil")
	}
	if bs.wm.Config == nil {
		return nil, fmt.Errorf("wallet manager config is nil")
	}
	result := make([]*types.Balance, 0, len(address))
	for _, addr := range address {
		bal, err := bs.wm.GetAddrBalance(addr, "latest")
		if err != nil {
			continue
		}
		balStr := util.BigIntToDecimal(bal, bs.wm.SymbolDecimal())
		result = append(result, &types.Balance{
			Symbol:           bs.wm.Config.Symbol,
			Address:          addr,
			Balance:          balStr,
			ConfirmBalance:   balStr,
			UnconfirmBalance: "0",
		})
	}
	return result, nil
}

// GetBlockchainSyncStatus 返回链同步状态：网络最新高度、当前扫描高度与是否同步中。
func (bs *EthBlockScanner) GetBlockchainSyncStatus() (*types.BlockchainSyncStatus, error) {
	network := bs.GetGlobalMaxBlockHeight()
	current := bs.GetScannedBlockHeight()
	status := &types.BlockchainSyncStatus{
		NetworkBlockHeight: network,
		CurrentBlockHeight: current,
		Syncing:            current < network,
	}
	return status, nil
}

// ExtractTransactionData 仅提取与扫描目标相关的主币和 ERC20 代币转账交易单（按 txid 查询）。
// 为简化实现，内部直接调用 ExtractTransactionAndReceiptData，并丢弃合约回执结果。
func (bs *EthBlockScanner) ExtractTransactionData(txid string, scanTargetFunc adaptscanner.BlockScanTargetFunc) (map[string][]*types.TxExtractData, error) {
	data, _, err := bs.ExtractTransactionAndReceiptData(txid, scanTargetFunc)
	return data, err
}

// padTo32Bytes 工具函数：将字节切片左补零至 32 字节（用于 ABI uint256 解析）
func padTo32Bytes(b []byte) []byte {
	if len(b) >= 32 {
		return b[len(b)-32:] // 截断高位（理论上不应发生）
	}
	padded := make([]byte, 32)
	copy(padded[32-len(b):], b)
	return padded
}

// extractAddressFromTopic 从 32 字节 topic 十六进制串中提取地址（后 20 字节）。
// 兼容带 0x/0X 前缀（66 字符）与无前缀（64 字符），去前缀后必须恰好 64 字符否则返回空（白名单校验）。
// 不做 hex 解码校验：合规节点返回的 topic 均为合法十六进制，安全边界在节点层；len(s)==64 已足够过滤异常长度。
func extractAddressFromTopic(topic string) string {
	s := strings.TrimSpace(topic)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		s = s[2:]
	}
	if len(s) != 64 {
		return ""
	}
	return "0x" + strings.ToLower(s[24:]) // 后 40 hex = 20 字节地址
}

// ExtractTransactionAndReceiptData 提取与扫描目标相关的主币和 ERC20 代币转账交易单及合约回执。
// - 主币：根据 from/to/value 构造 Transaction；
// - Token：解析 ERC20 Transfer 事件日志，根据 from/to/amount 构造 Transaction；
// - 使用 scanTargetFunc 过滤只与目标地址/别名相关的记录，并按 SourceKey 聚合。
func (bs *EthBlockScanner) ExtractTransactionAndReceiptData(txid string, scanTargetFunc adaptscanner.BlockScanTargetFunc) (map[string][]*types.TxExtractData, map[string]*types.SmartContractReceipt, error) {
	if bs.wm == nil || bs.wm.Client == nil {
		return nil, nil, fmt.Errorf("wallet manager or rpc client is nil")
	}
	if scanTargetFunc == nil {
		return nil, nil, fmt.Errorf("scan target func is nil")
	}
	if bs.wm.Config == nil {
		return nil, nil, fmt.Errorf("wallet manager config is nil")
	}

	// 1. 获取交易与回执
	txRes, err := bs.wm.Client.Call("eth_getTransactionByHash", []interface{}{txid})
	if err != nil {
		return nil, nil, err
	}
	if !txRes.Exists() || txRes.Type == 0 {
		return nil, nil, fmt.Errorf("transaction not found")
	}

	rcRes, err := bs.wm.Client.Call("eth_getTransactionReceipt", []interface{}{txid})
	if err != nil {
		return nil, nil, err
	}
	if !rcRes.Exists() || rcRes.Type == 0 {
		return nil, nil, fmt.Errorf("transaction receipt not found")
	}

	// 2. 解析基础字段（仅使用回执已上链的交易；pending 交易由上层单独处理）
	from := strings.ToLower(txRes.Get("from").String())
	to := strings.ToLower(txRes.Get("to").String()) // 可能为空（合约创建）
	valueHex := txRes.Get("value").String()
	blockHash := txRes.Get("blockHash").String()
	blockNumberHex := txRes.Get("blockNumber").String()

	var blockHeight uint64
	if blockNumberHex != "" {
		if h, err := hexutil.DecodeUint64(blockNumberHex); err == nil {
			blockHeight = h
		}
	}

	// value 解析为主币金额
	amount := big.NewInt(0)
	if valueHex != "" && valueHex != "0x" {
		if v, err := hexutil.DecodeBig(valueHex); err == nil {
			amount = v
		}
	}
	amountStr := util.BigIntToDecimal(amount, bs.wm.SymbolDecimal())

	// 手续费 = gasUsed * effectiveGasPrice (fall back 到 gasPrice)
	var fee *big.Int = big.NewInt(0)
	if gasUsedHex := rcRes.Get("gasUsed").String(); gasUsedHex != "" {
		if gasUsed, err := hexutil.DecodeBig(gasUsedHex); err == nil {
			var gasPrice *big.Int
			if gp := txRes.Get("effectiveGasPrice").String(); gp != "" {
				gasPrice, _ = hexutil.DecodeBig(gp)
			}
			if gasPrice == nil {
				if gp := txRes.Get("gasPrice").String(); gp != "" {
					gasPrice, _ = hexutil.DecodeBig(gp)
				}
			}
			if gasPrice != nil {
				fee = new(big.Int).Mul(gasUsed, gasPrice)
			}
		}
	}
	feeStr := util.BigIntToDecimal(fee, bs.wm.SymbolDecimal())

	// 默认视为失败，仅当回执 status 明确为 0x1 时视为成功（EIP-658）
	status := types.TxStatusFail
	if statusHex := rcRes.Get("status").String(); statusHex != "" {
		if st, err := hexutil.DecodeUint64(statusHex); err == nil && st == 1 {
			status = types.TxStatusSuccess
		}
	}

	// 性能优化与安全性：失败交易通常无需解析主币/代币日志，直接丢弃，避免上层误入账失败交易
	if status == types.TxStatusFail {
		return nil, nil, nil
	}

	// 入账安全前提：仅当交易已上链（有区块高度与哈希）时才输出，避免 pending 或异常数据被误当作已确认交易入账
	if blockHeight == 0 || blockHash == "" {
		return nil, nil, nil
	}

	// 入账前提：仅当交易已上链（有区块高度与哈希）时才输出，避免 pending 或异常数据被入账
	if blockHeight == 0 || blockHash == "" {
		return nil, nil, nil
	}

	nowUnix := time.Now().Unix()

	result := make(map[string][]*types.TxExtractData)
	// 每个 Transfer 事件一条回执，key 用 contractAddr:logIndex 避免同合约多笔转账只保留一条
	contractReceipts := make(map[string]*types.SmartContractReceipt)

	// 构造主币 Coin
	ethCoin := types.Coin{
		Symbol:     bs.wm.Config.Symbol,
		IsContract: false,
	}

	// 3. 处理主币转账：
	// - 只要 value > 0 就视为一笔主币转账（不关心是否普通转账或合约调用时附带 value）；
	// - to 可能为空（合约创建），此时仅对 from 做出账记录，并在 ExtParam 中显式标记，便于上层识别。
	if amount.Sign() > 0 && from != "" {
		actualTo := to
		toList := []string{}
		if actualTo != "" {
			toList = []string{actualTo + ":" + amountStr}
		}

		isContractCreation := actualTo == ""
		var ext map[string]string
		if isContractCreation {
			ext = map[string]string{"contract_creation": "true"}
		}

		// 监控发送方（from）
		fromParam := types.NewScanTargetParamForAddress(bs.wm.Config.Symbol, from)
		fromRes := scanTargetFunc(fromParam)
		if fromRes.Exist {
			txObj := &types.Transaction{
				TxID:        txid,
				Coin:        ethCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      amountStr,
				Fees:        feeStr,
				From:        []string{from + ":" + amountStr},
				To:          toList,
				Status:      status,
				ConfirmTime: nowUnix,
				ExtParam:    ext,
			}
			data := types.NewTxExtractData()
			data.Transaction = txObj
			result[fromRes.SourceKey] = append(result[fromRes.SourceKey], data)
		}

		// 监控接收方（to），仅当 to 已知
		if actualTo != "" {
			toParam := types.NewScanTargetParamForAddress(bs.wm.Config.Symbol, actualTo)
			toRes := scanTargetFunc(toParam)
			if toRes.Exist {
				txObj := &types.Transaction{
					TxID:        txid,
					Coin:        ethCoin,
					BlockHash:   blockHash,
					BlockHeight: blockHeight,
					Amount:      amountStr,
					Fees:        feeStr,
					From:        []string{from + ":" + amountStr},
					To:          toList,
					Status:      status,
					ConfirmTime: nowUnix,
					ExtParam:    ext,
				}
				data := types.NewTxExtractData()
				data.Transaction = txObj
				result[toRes.SourceKey] = append(result[toRes.SourceKey], data)
			}
		}
	}

	// 4. 解析 ERC20 Transfer 事件日志：
	// - 仅识别标准 Transfer 事件 (topic0 固定为 transferTopic)；
	// - 同一交易内同一合约的多笔 Transfer 均会被解析（包括批量转账合约）。
	const transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	logs := rcRes.Get("logs").Array()

	for logIdx, lg := range logs {
		if !lg.IsObject() {
			continue
		}
		contractAddr := strings.ToLower(lg.Get("address").String())
		topics := lg.Get("topics").Array()
		dataHex := lg.Get("data").String()

		if len(topics) == 0 || !strings.EqualFold(topics[0].String(), transferTopic) {
			continue
		}

		var fromToken, toToken string
		var tokenAmount *big.Int = big.NewInt(0)

		// Case 1: both from and to are indexed（最常见的 ERC20 Transfer 形式：topics[1]=from, topics[2]=to, data=amount）
		if len(topics) >= 3 {
			fromToken = extractAddressFromTopic(topics[1].String())
			toToken = extractAddressFromTopic(topics[2].String())
			if fromToken != "" && toToken != "" {
				// Parse amount from data (ensure 32 bytes)
				if dataHex != "" && dataHex != "0x" {
					if b, err := hexutil.Decode(dataHex); err == nil {
						tokenAmount = new(big.Int).SetBytes(padTo32Bytes(b))
					}
				}
			}
		} else if len(topics) >= 2 {
			// Case 2: 仅 from 被 indexed，to + amount 存在 data 中（结构：前 32 字节为右对齐的 to 地址，后 32 字节为 amount）
			fromToken = extractAddressFromTopic(topics[1].String())
			if fromToken == "" {
				continue // 无效 topic，跳过后续 data 解析
			}
			if dataHex != "" && dataHex != "0x" {
				if b, err := hexutil.Decode(dataHex); err == nil && len(b) >= 32 {
					// First 32 bytes: to address (right-aligned, 12 zeros + 20-byte address)
					addrBytes := b[12:32]
					if len(addrBytes) == 20 {
						toToken = "0x" + strings.ToLower(hex.EncodeToString(addrBytes))
					}
					// Last 32 bytes: amount
					if len(b) >= 64 {
						tokenAmount = new(big.Int).SetBytes(b[32:64])
					}
				}
			}
		}

		if tokenAmount == nil || tokenAmount.Sign() <= 0 || fromToken == "" || toToken == "" {
			continue
		}

		// 严格依赖精度：查询不到 decimals 时跳过该 token 事件，避免金额被按错误精度入账
		tokenDecimals, ok := bs.getTokenDecimals(contractAddr)
		if !ok || tokenDecimals <= 0 {
			fmt.Printf("[EthBlockScanner] skip ERC20 Transfer for contract %s due to unknown decimals\n", contractAddr)
			continue
		}
		tokenAmountStr := util.BigIntToDecimal(tokenAmount, tokenDecimals)

		tokenCoin := types.Coin{
			Symbol:     contractAddr,
			IsContract: true,
		}

		// from 持币地址
		fromParam := types.NewScanTargetParamForAddress(contractAddr, fromToken)
		fromRes := scanTargetFunc(fromParam)
		if fromRes.Exist {
			txObj := &types.Transaction{
				TxID:        txid,
				Coin:        tokenCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      tokenAmountStr,
				Decimal:     tokenDecimals,
				Fees:        "0",
				From:        []string{fromToken + ":" + tokenAmountStr},
				To:          []string{toToken + ":" + tokenAmountStr},
				Status:      status,
				ConfirmTime: nowUnix,
				// ExtParam 用于携带事件定位信息，便于外部系统做严格复核（如按 logIndex 对齐）。
				ExtParam: map[string]string{"logIndex": strconv.Itoa(logIdx)},
			}
			data := types.NewTxExtractData()
			data.Transaction = txObj
			result[fromRes.SourceKey] = append(result[fromRes.SourceKey], data)
		}

		// to 持币地址
		toParam := types.NewScanTargetParamForAddress(contractAddr, toToken)
		toRes := scanTargetFunc(toParam)
		if toRes.Exist {
			txObj := &types.Transaction{
				TxID:        txid,
				Coin:        tokenCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      tokenAmountStr,
				Decimal:     tokenDecimals,
				Fees:        "0",
				From:        []string{fromToken + ":" + tokenAmountStr},
				To:          []string{toToken + ":" + tokenAmountStr},
				Status:      status,
				ConfirmTime: nowUnix,
				ExtParam:    map[string]string{"logIndex": strconv.Itoa(logIdx)},
			}
			data := types.NewTxExtractData()
			data.Transaction = txObj
			result[toRes.SourceKey] = append(result[toRes.SourceKey], data)
		}

		// 每个 Transfer 事件一条回执；key = txid:contractAddr:logIndex，避免跨交易合并结果集时发生 key 碰撞
		receiptKey := txid + ":" + contractAddr + ":" + strconv.Itoa(logIdx)
		contractReceipts[receiptKey] = &types.SmartContractReceipt{
			Coin:        tokenCoin,
			TxID:        txid,
			From:        fromToken,
			To:          toToken,
			Value:       tokenAmountStr,
			Fees:        "0",
			BlockHash:   blockHash,
			BlockHeight: blockHeight,
			Status:      status,
			ExtParam:    map[string]string{"logIndex": strconv.Itoa(logIdx)},
		}
	}

	return result, contractReceipts, nil
}
