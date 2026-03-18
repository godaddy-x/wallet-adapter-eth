package scanner

// EthBlockScanner：以太坊 / EVM 区块扫描器实现。
//
// 设计目标：
// 1. 扫块层（Block-level）：
//    - 按高度从节点拉取完整区块头和交易（eth_getBlockByNumber）
//    - 通过 ScanBlock / ScanBlockWithResult 向上层输出 BlockHeader 与提取统计
//    - 通过 Base.NewBlockNotify / 观察者接口支持异步订阅模式（兼容旧架构）
//
// 2. 交易提取层（Tx-level）：
//    - 按 txid 精准提取主币与 ERC20 代币转账（ExtractTransactionAndReceiptData）
//    - 主币：根据 tx.from / tx.to / value 构造 Transaction
//    - Token：解析标准 ERC20 Transfer 事件（topics + data）构造 Transaction
//    - 手续费：为 tx.from 所属账户生成独立 GAS 记录（FeeType="gas"），避免在多条 Token 记录上重复记费
//    - 通过 ScanTargetFunc 仅对关心的地址/账户（AccountID=SourceKey）生成记录，并按 SourceKey 聚合
//
// 3. 验证与对账层（Verify-level）：
//    - VerifyTransactionByTxID：按 txid + minConfirmations 从链上二次复核（存在 / 成功 / 确认数），
//      并重新跑提取逻辑，返回可入账结果集 TxVerifyResult
//    - VerifyTransactionMatch：在 VerifyTransactionByTxID 基础上，与业务期望对象 TxVerifyExpected 严格比对
//      （主币比对 from/to/value，代币基于 txid+contractAddr+logIndex 精确定位单条 Transfer）
//
// 4. 持续扫描循环（Cursor-level）：
//    - RunScanLoop：从指定起始高度 startHeight 开始（inclusive），持续扫描至 safeTo=latest-confirmations，
//      可选 windowSize 用于回填最近若干安全块以覆盖重组；每扫完一个高度将 BlockScanResult 同步回调给外部。
//
// 5. 性能与缓存：
//    - TxExtractConcurrency：控制单区块内按 txhash 并行提取的 goroutine 数量
//    - tokenDecimalsCache：按合约地址缓存 ERC20 decimals，>0 表示有效精度，0 表示“查询失败/非标准 ERC20”，调用方必须跳过
//    - blockTimestampCache：按 blockHash 缓存区块时间戳（秒），带上限与 FIFO 淘汰，避免重复 eth_getBlockByHash 与内存膨胀
//
// 整体原则：
// - 正确性优先：宁可跳过未知精度的 Token / 失败交易 / 未上链交易，也不猜测或“硬入账”
// - 统一口径：所有 Tx 提取与 Verify 均复用 ExtractTransactionAndReceiptData，保证“扫块入口”和“按 txid 复核入口”结果一致
// - 成本可控：仅在命中 ScanTargetFunc 的 tx 上跑深度提取；通过缓存与窗口策略减少重复 RPC

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
// - 嵌入 Base，复用扫描任务调度；
// - 通过 WalletManager.RPC 查询最新区块高度与区块详情；
// - 提供 ExtractTransactionAndReceiptData 按 txid 精准提取主币与 ERC20 交易单。
type EthBlockScanner struct {
	*adaptscanner.Base
	// wm 为链上 RPC 与元数据能力的入口（如 SymbolDecimal、ERC20Metadata）
	wm *manager.WalletManager

	// scannedHeight 为 Run() 周期任务使用的内存游标（不做持久化）。
	// 设计目标：仅用于“自动追块”场景；更推荐外部系统使用 RunScanLoop 自行维护游标与回填窗口。
	scannedHeight   uint64
	scannedHeightMu sync.RWMutex

	// scanLoopPause 用于暂停/恢复正在运行的 RunScanLoop。
	// - Pause/Stop：将 paused=true，并 close(pauseCh) 以打断 interval 等待；
	// - Run/Restart：将 paused=false，重置 pauseCh，并唤醒阻塞在 cond.Wait 的扫描循环。
	scanLoopMu      sync.Mutex
	scanLoopCond    *sync.Cond
	scanLoopPaused  bool
	scanLoopPauseCh chan struct{}

	// TxExtractConcurrency 控制单个区块内并行提取交易的并发度。
	// 建议根据节点吞吐与限流策略调整（如 5/10/20）。<=0 时使用默认值。
	TxExtractConcurrency int

	// tokenDecimalsCache 按合约地址缓存 ERC20 精度，避免每个事件都发 ERC20Metadata RPC
	// 约定：>0 为有效精度；=0 表示“查询失败/非标准 ERC20”，调用方需跳过该事件，绝不猜测默认 18。
	tokenDecimalsCache   map[string]int32
	tokenDecimalsCacheMu sync.RWMutex

	// blockTimestampCache 按 blockHash 缓存区块时间戳，避免同一高度下多次 RPC。
	blockTimestampCache   map[string]int64
	blockTimestampCacheMu sync.RWMutex

	// blockTimestampCacheMax 限制缓存条目数，避免长期扫描导致内存无界增长。
	// 采用 FIFO 淘汰策略（简单且足够用）。
	blockTimestampCacheMax   int
	blockTimestampCacheOrder []string
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
		// 事件精确定位：LogIndex 必须与 expected 完全一致
		if r.LogIndex != tr.LogIndex {
			out.Mismatches = append(out.Mismatches, "token logIndex mismatch")
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
		Symbol:           "",
		Height:           height,
		Success:          false,
		FailedTxIDs:      make([]string, 0),
		TxTotal:          0,
		TxFailed:         0,
		ExtractedTxs:     0,
		ExtractData:      make(map[string][]*types.TxExtractData),
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
			txHash           string
			extractData      map[string][]*types.TxExtractData
			contractReceipts map[string]*types.SmartContractReceipt
			err              error
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

// NewBlockScanner 创建以太坊区块扫描器。
// TokenMetadataFunc 现由外部在 Base 上统一注入（与 ScanTargetFunc 一致），此处仅依赖其存在而不再单独持有。
func NewBlockScanner(wm *manager.WalletManager) *EthBlockScanner {
	bs := &EthBlockScanner{
		Base:                   adaptscanner.NewBlockScannerBase(),
		wm:                     wm,
		TxExtractConcurrency:   10,
		tokenDecimalsCache:     make(map[string]int32),
		blockTimestampCache:    make(map[string]int64),
		blockTimestampCacheMax: 1024,
	}
	bs.scanLoopCond = sync.NewCond(&bs.scanLoopMu)
	bs.scanLoopPauseCh = make(chan struct{})
	// Run() 周期任务：自动追块（不含 confirmations/window），适用于简单场景；
	// 更推荐外部系统使用 RunScanLoop 维护游标与回填窗口。
	bs.SetTask(bs.scanBlockTask)
	// 初始化游标为当前最新高度，避免首次 Run() 从 0 开始全量扫历史
	if wm != nil && wm.Client != nil {
		bs.scannedHeight = bs.GetGlobalMaxBlockHeight()
	}
	return bs
}

func (bs *EthBlockScanner) getPauseCh() <-chan struct{} {
	bs.scanLoopMu.Lock()
	ch := bs.scanLoopPauseCh
	bs.scanLoopMu.Unlock()
	return ch
}

func (bs *EthBlockScanner) blockIfPaused() {
	bs.scanLoopMu.Lock()
	for bs.scanLoopPaused {
		bs.scanLoopCond.Wait()
	}
	bs.scanLoopMu.Unlock()
}

// Run 恢复（或保持）RunScanLoop 的运行状态。
// 注意：RunScanLoop 本身仍需由调用方启动（通常在 goroutine 中）。
func (bs *EthBlockScanner) Run() error {
	bs.scanLoopMu.Lock()
	if bs.scanLoopPaused {
		bs.scanLoopPaused = false
		// 重置 pauseCh，供下次 Pause 打断等待
		bs.scanLoopPauseCh = make(chan struct{})
		bs.scanLoopCond.Broadcast()
	}
	bs.scanLoopMu.Unlock()
	return nil
}

// Pause 暂停 RunScanLoop：让扫描循环进入阻塞等待，直到再次调用 Run/Restart。
func (bs *EthBlockScanner) Pause() error {
	bs.scanLoopMu.Lock()
	if !bs.scanLoopPaused {
		bs.scanLoopPaused = true
		// close 以打断 interval 等待（select <-pauseCh）
		if bs.scanLoopPauseCh != nil {
			close(bs.scanLoopPauseCh)
		}
	}
	bs.scanLoopMu.Unlock()
	return nil
}

func (bs *EthBlockScanner) getScannedHeight() uint64 {
	bs.scannedHeightMu.RLock()
	defer bs.scannedHeightMu.RUnlock()
	return bs.scannedHeight
}

func (bs *EthBlockScanner) setScannedHeight(h uint64) {
	bs.scannedHeightMu.Lock()
	bs.scannedHeight = h
	bs.scannedHeightMu.Unlock()
}

// scanBlockTask 自动追块任务：从 scannedHeight+1 扫到 latest；中途失败则停止在失败高度，等待下个 tick 重试。
func (bs *EthBlockScanner) scanBlockTask() {
	if bs.wm == nil || bs.wm.Client == nil {
		return
	}
	latest := bs.GetGlobalMaxBlockHeight()
	if latest == 0 {
		return
	}
	current := bs.getScannedHeight()
	if current >= latest {
		return
	}
	for h := current + 1; h <= latest; h++ {
		res, err := bs.ScanBlockWithResult(h)
		if err != nil {
			return
		}
		if res == nil || !res.Success {
			return
		}
		bs.setScannedHeight(h)
	}
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

// RunScanLoop 从外部指定的 startHeight 开始（inclusive），持续按高度向上扫描，并始终只扫描已满足 confirmations 的安全范围。
//   - 设定 safeTo = latest - confirmations（latest 为链上最新高度）
//   - 扫描高度在每轮循环中包含一个回填窗口：从 windowFrom = max(0, safeTo-windowSize) 扫到 safeTo
//     为了捕获潜在重组，这个回填窗口在每轮都会重新扫描（即使已扫过）。
//   - 每扫完一个区块高度就同步调用 handleBlock（若 handleBlock 为 nil 则忽略）。
func (bs *EthBlockScanner) RunScanLoop(
	startHeight, confirmations, windowSize uint64,
	interval time.Duration,
	handleBlock func(res *types.BlockScanResult),
) error {
	if bs.wm == nil || bs.wm.Client == nil {
		return fmt.Errorf("wallet manager or rpc client is nil")
	}
	if bs.wm.Config == nil {
		return fmt.Errorf("wallet manager config is nil")
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}

	// cursor 表示“已处理到的安全高度”（safeTo）。
	// 为了让第一次扫描命中 startHeight，我们把 cursor 初始化到 startHeight-1。
	// 当 startHeight==0 时，cursor 保持为 0，第一次扫描从 0 开始（inclusive）。
	var cursor uint64
	if startHeight == 0 {
		cursor = 0
	} else {
		cursor = startHeight - 1
	}

	for {
		bs.blockIfPaused()

		latest := bs.GetGlobalMaxBlockHeight()
		if latest == 0 {
			// 等待 interval，但允许 Pause 立即打断并进入阻塞
			select {
			case <-time.After(interval):
			case <-bs.getPauseCh():
			}
			continue
		}

		// confirmations 足够时，safeTo 才有意义
		var safeTo uint64
		if latest > confirmations {
			safeTo = latest - confirmations
		} else {
			safeTo = 0
		}
		// 为了持续覆盖潜在重组，需要当 safeTo 不变时也继续对回填窗口进行重扫，
		// 因此这里仅在 safeTo=0 时跳过。
		if safeTo == 0 {
			select {
			case <-time.After(interval):
			case <-bs.getPauseCh():
			}
			continue
		}

		// 计算回填窗口起点；窗口范围每轮都会重新扫以捕获潜在重组。
		windowFrom := uint64(0)
		if windowSize > 0 {
			if safeTo > windowSize {
				windowFrom = safeTo - windowSize
			} else {
				windowFrom = 0
			}
		}

		// 默认从 cursor+1 开始扫；若 windowFrom 落在 cursor 之后（或更靠近链头），则扩展为从 windowFrom 开始回填。
		scanFrom := cursor + 1
		// 仅当 windowSize>0 才允许回填已扫描过的高度。
		// windowSize=0 时 scanFrom 始终等于 cursor+1，保证不会重复扫历史安全高度。
		if windowSize > 0 && windowFrom < scanFrom {
			scanFrom = windowFrom
		}

		// 扫描区间 [scanFrom..safeTo]：
		// - 扫描失败不退出 loop；
		// - 当某高度失败时，停留在该高度持续重试（不推进 cursor），并将失败结果回调给业务侧；
		// - 业务侧可根据 res.ErrorReason 决定告警、跳过高度或调整策略。
		completed := true
		for h := scanFrom; h <= safeTo; h++ {
			bs.blockIfPaused()

			res, err := bs.ScanBlockWithResult(h)
			if err != nil {
				completed = false
				// 构造一个“失败也可回调”的结果对象，尽量补齐 header/hash，便于外部定位。
				if res == nil {
					res = &types.BlockScanResult{
						Symbol:           "",
						Height:           h,
						Success:          false,
						ErrorReason:      err.Error(),
						ExtractData:      make(map[string][]*types.TxExtractData),
						ContractReceipts: make(map[string]*types.SmartContractReceipt),
						FailedTxIDs:      make([]string, 0),
					}
					if bs.wm != nil && bs.wm.Config != nil {
						res.Symbol = bs.wm.Config.Symbol
					}
				} else {
					res.Success = false
					if res.ErrorReason == "" {
						res.ErrorReason = err.Error()
					}
				}
				// network height（本轮 latest）用于业务侧观测进度与告警
				res.NetworkBlockHeight = latest

				// 预先构建最小 Header：至少带 Height/Symbol/Confirmations（基于 latest 计算），便于业务侧展示/判断。
				if res.Header == nil {
					var confs uint64
					if latest >= h {
						confs = latest - h + 1
					}
					res.Header = &types.BlockHeader{
						Height:        h,
						Confirmations: confs,
					}
					if bs.wm != nil && bs.wm.Config != nil {
						res.Header.Symbol = bs.wm.Config.Symbol
					}
				}

				// 再尽力补齐 hash/parentHash/time：拉取轻量区块头（false），避免重复解析整块。
				if res.Header == nil && bs.wm != nil && bs.wm.Client != nil && bs.wm.Config != nil {
					tag := fmt.Sprintf("0x%x", h)
					blk, e := bs.wm.Client.Call("eth_getBlockByNumber", []interface{}{tag, false})
					if e == nil {
						numberHex := blk.Get("number").String()
						hash := blk.Get("hash").String()
						parentHash := blk.Get("parentHash").String()
						timeHex := blk.Get("timestamp").String()
						var ts uint64
						if timeHex != "" {
							if tsv, ee := hexutil.DecodeUint64(timeHex); ee == nil {
								ts = tsv
							}
						}
						var hh uint64
						if numberHex != "" {
							if hv, ee := hexutil.DecodeUint64(numberHex); ee == nil {
								hh = hv
							}
						}
						if hash != "" && hh != 0 {
							res.BlockHash = hash
							res.Header = &types.BlockHeader{
								Hash:              hash,
								Previousblockhash: parentHash,
								Height:            hh,
								Time:              ts,
								Symbol:            bs.wm.Config.Symbol,
								Confirmations: func() uint64 {
									if latest >= hh {
										return latest - hh + 1
									}
									return 0
								}(),
							}
						}
					}
				}

				if handleBlock != nil {
					handleBlock(res)
				}

				// 将 cursor 更新到 h-1：下一轮从 h 开始重试，避免重复扫更早高度。
				if h == 0 {
					cursor = 0
				} else if h-1 > cursor {
					cursor = h - 1
				}

				// 停留在该高度重试
				select {
				case <-time.After(interval):
				case <-bs.getPauseCh():
				}
				break
			}

			if handleBlock != nil {
				// network height（本轮 latest）用于业务侧观测进度与告警
				if res != nil {
					res.NetworkBlockHeight = latest
				}
				handleBlock(res)
			}
		}

		// 将游标推进到当前轮的 safeTo；后续重组由回填窗口再次扫描覆盖。
		// 只有当本轮完整扫过至 safeTo 时才推进；若中途失败 break，则停留在失败高度重试。
		if completed {
			cursor = safeTo
		}
		select {
		case <-time.After(interval):
		case <-bs.getPauseCh():
		}
	}
}

// ScanBlockOnce 指定高度扫描一次（用于补扫/漏扫修复）。
// 说明：
// - 不影响 RunScanLoop 的内部 cursor 语义（RunScanLoop 由外部系统维护游标）；
// - 会补齐 NetworkBlockHeight 与 Header.Confirmations，便于业务侧做一致性判断。
func (bs *EthBlockScanner) ScanBlockOnce(height uint64) (*types.BlockScanResult, error) {
	res, err := bs.ScanBlockWithResult(height)
	latest := bs.GetGlobalMaxBlockHeight()
	if res != nil {
		res.NetworkBlockHeight = latest
		if res.Header != nil {
			// confirmations 基于“本次查询时刻”的 latest 计算
			if latest >= res.Header.Height && res.Header.Height > 0 {
				res.Header.Confirmations = latest - res.Header.Height + 1
			}
		}
	}
	return res, err
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

// padTo32Bytes 工具函数：将字节切片左补零至 32 字节（用于 ABI uint256 解析）
func padTo32Bytes(b []byte) []byte {
	if len(b) >= 32 {
		return b[len(b)-32:] // 截断高位（理论上不应发生）
	}
	padded := make([]byte, 32)
	copy(padded[32-len(b):], b)
	return padded
}

// getBlockTimestamp 根据 blockHash 获取区块时间戳（秒），并做本地缓存。
// fallback 用于 RPC 失败或节点返回异常数据时兜底（不做硬失败，保证扫描可继续）。
func (bs *EthBlockScanner) getBlockTimestamp(blockHash string, fallback int64) int64 {
	if blockHash == "" || bs.wm == nil || bs.wm.Client == nil {
		return fallback
	}

	bs.blockTimestampCacheMu.RLock()
	if ts, ok := bs.blockTimestampCache[blockHash]; ok && ts > 0 {
		bs.blockTimestampCacheMu.RUnlock()
		return ts
	}
	bs.blockTimestampCacheMu.RUnlock()

	// eth_getBlockByHash 不返回 full tx 列表，降低 payload。
	blkRes, err := bs.wm.Client.Call("eth_getBlockByHash", []interface{}{blockHash, false})
	if err != nil {
		return fallback
	}
	tsHex := blkRes.Get("timestamp").String()
	if tsHex == "" || tsHex == "0x" {
		return fallback
	}
	tsUint, err := hexutil.DecodeUint64(tsHex)
	if err != nil || tsUint == 0 {
		return fallback
	}
	ts := int64(tsUint)

	bs.blockTimestampCacheMu.Lock()
	defer bs.blockTimestampCacheMu.Unlock()

	// 写入前再次检查，避免并发重复写。
	if existing, ok := bs.blockTimestampCache[blockHash]; ok && existing != 0 {
		return existing
	}

	// 插入前进行 FIFO 淘汰，确保缓存大小不超过上限。
	if bs.blockTimestampCacheMax > 0 {
		for len(bs.blockTimestampCache) >= bs.blockTimestampCacheMax && len(bs.blockTimestampCacheOrder) > 0 {
			oldest := bs.blockTimestampCacheOrder[0]
			bs.blockTimestampCacheOrder = bs.blockTimestampCacheOrder[1:]
			delete(bs.blockTimestampCache, oldest)
		}
	}

	bs.blockTimestampCache[blockHash] = ts
	bs.blockTimestampCacheOrder = append(bs.blockTimestampCacheOrder, blockHash)
	return ts
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

// ExtractTransactionAndReceiptData 按 txid 精准提取“可入账交易单 + 合约回执”，并按 SourceKey(AccountID) 聚合。
//
// 提取规则：
// 1. 过滤条件：
//   - 交易必须已上链（blockNumber / blockHash 非空），否则直接返回 nil（pending 由上层单独处理）
//   - 回执 status 必须为成功（EIP-658: status==0x1），否则直接返回 nil，避免失败交易被误入账
//
// 2. 主币（ETH）：
//   - 根据 tx.from / tx.to / value 解析出主币金额（按 SymbolDecimal() 格式化为十进制字符串）
//   - 仅当 value>0 且 from 非空时才视为一笔主币转账
//   - 通过 ScanTargetFunc(symbol, from/to) 判断地址归属哪个 AccountID(SourceKey)：
//   - 若 from 与 to 归属同一 SourceKey：只生成一条记录（From=[from:amount], To=[to:amount]）
//   - 若归属不同 SourceKey：from 账户生成出账记录（From 有值, To 仅在同源时填写），to 账户生成纯入账记录（To 有值, From 为空）
//   - 主币记录本身不再重复记录手续费（Fees="0"），手续费统一通过 GAS 记录归集
//   - 若 to 为空（合约创建），在 ExtParam["contract_creation"]="true" 标记，便于上层识别
//
// 3. 手续费（GAS）：
//   - 手续费按 gasUsed * effectiveGasPrice（fallback 到 gasPrice）计算为 feeWei，并按 SymbolDecimal() 格式化为 feeStr
//   - 无论是否包含主币转账，只要 tx 成功且 tx.from 命中 ScanTargetFunc，都会为该 AccountID 生成一条独立 GAS 记录：
//   - Coin = 原生币（ethCoin）
//   - Amount = "0"
//   - Fees   = feeStr
//   - FeeType = "gas"
//   - From   = [from:feeStr]，To 为空
//   - 保证同一 AccountID+txid 最多仅一条记录包含非零 Fees，避免在多条业务记录上重复记费
//
// 4. ERC20 代币（Token）：
//   - 从回执 logs 中筛选标准 ERC20 Transfer 事件（topic0 固定为 Transfer(address,address,uint256)）
//   - Case1：topics[1]=from, topics[2]=to, data=amount
//   - 通过 extractAddressFromTopic 从 32 字节 topic 中提取地址
//   - Case2：topics[1]=from, data 前 32 字节为右对齐的 to 地址，后 32 字节为 amount
//   - amount 按 uint256 从 data 解码，并通过 getTokenDecimals(contractAddr) 获取 decimals：
//   - 优先使用业务注入的 TokenMetadataFunc(symbol, contractAddr)
//   - 否则回退链上 ERC20Metadata(contractAddr)
//   - decimals<=0 或查询失败时跳过该事件（绝不猜测默认 18）
//   - 通过 ScanTargetFunc(contractAddr, fromToken/toToken) 判断代币归属的 AccountID：
//   - from 命中：在该 SourceKey 下生成一条 From=[fromToken:amountStr], To=[toToken:amountStr] 的记录
//   - to 命中：在该 SourceKey 下同样生成一条记录（不会与 from 合并，便于按 AccountID 维度独立入账）
//   - Token 记录 Fees 始终为 "0"，手续费由 GAS 记录负责
//   - 每条 Transfer 事件同时生成一条 SmartContractReceipt，key=txid:contractAddr:logIndex，用于 VerifyTransactionMatch 精确定位
//
// 5. 聚合维度：
//   - 返回值 result:  map[SourceKey][]*TxExtractData ；SourceKey 通常是 AccountID
//   - ContractReceipts: map[txid:contractAddr:logIndex]*SmartContractReceipt
//   - 上层可按 SourceKey 维度扫描账务，也可按 txid+logIndex 做逐条严格比对
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

	confirmTime := bs.getBlockTimestamp(blockHash, time.Now().Unix())

	result := make(map[string][]*types.TxExtractData)
	// 每个 Transfer 事件一条回执，key 用 contractAddr:logIndex 避免同合约多笔转账只保留一条
	contractReceipts := make(map[string]*types.SmartContractReceipt)

	// 构造主币 Coin
	ethCoin := types.Coin{
		Symbol:     bs.wm.Config.Symbol,
		IsContract: false,
	}

	// 3.1 处理手续费记录（独立 GAS 记录方案）
	// 无论是否包含主币转账，只要 tx 成功且 tx.from 是监控目标，就为 payer 生成一条独立的 GAS 记录：
	// - Coin 为原生币（ethCoin）
	// - Amount 固定为 "0"
	// - Fees 为本次 tx 的全部 gas 费
	// - FeeType="gas"
	// 主币/Token 转账记录本身不再重复记录手续费（Fees="0"），避免下游汇总时被放大。
	// 定性依据：
	// - fee payer 永远是顶层 tx.from
	// - 归属口径：scanTargetFunc 返回的 SourceKey 代表 AccountID（账户ID），因此手续费只计入 payer 对应的账户。
	// - 手续费金额使用本次 tx 的 gasUsed * effectiveGasPrice（已计算为 feeStr）
	// - 通过 feeTxObj.FeeType="gas" 区分手续费记录
	if from != "" && fee != nil && fee.Sign() > 0 {
		fromFeeParam := types.NewScanTargetParamForAddress(bs.wm.Config.Symbol, from)
		fromFeeRes := scanTargetFunc(fromFeeParam)
		if fromFeeRes.Exist {
			feeTxObj := &types.Transaction{
				TxID:        txid,
				AccountID:   fromFeeRes.SourceKey,
				Coin:        ethCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      "0",
				Decimal:     bs.wm.SymbolDecimal(),
				Fees:        feeStr,
				From:        []string{from + ":" + feeStr},
				To:          []string{},
				Status:      status,
				ConfirmTime: confirmTime,
				FeeType:     "gas",
				ExtParam:    nil,
			}
			data := types.NewTxExtractData()
			data.Transaction = feeTxObj
			result[fromFeeRes.SourceKey] = append(result[fromFeeRes.SourceKey], data)
		}
	}

	// 3. 处理主币转账：
	// - 只要 value > 0 就视为一笔主币转账（不关心是否普通转账或合约调用时附带 value）；
	// - to 可能为空（合约创建），此时仅对 from 做出账记录，并在 ExtParam 中显式标记，便于上层识别。
	if amount.Sign() > 0 && from != "" {
		actualTo := to
		// 对齐“最终净额正负”口径：对同一个 sourceKey，只填入与该 sourceKey 对应的 input/output。
		// 这样下游用 TxInputs/TxOutputs 重算 amount（outputs-inputs）时，不会把不属于该 sourceKey 的另一侧也算进去。
		var toRes types.ScanTargetResult
		toResExist := false
		if actualTo != "" {
			toParam := types.NewScanTargetParamForAddress(bs.wm.Config.Symbol, actualTo)
			toRes = scanTargetFunc(toParam)
			toResExist = toRes.Exist
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
			toListForFrom := []string{}
			// 只有当 to 也属于同一个 sourceKey 时，才把它计入该 sourceKey 的 output。
			if toResExist && toRes.SourceKey == fromRes.SourceKey {
				toListForFrom = []string{actualTo + ":" + amountStr}
			}
			txObj := &types.Transaction{
				TxID:        txid,
				AccountID:   fromRes.SourceKey,
				Coin:        ethCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      amountStr,
				Decimal:     bs.wm.SymbolDecimal(),
				// 手续费通过独立 GAS 记录归集，此处不再重复记费。
				Fees:        "0",
				From:        []string{from + ":" + amountStr},
				To:          toListForFrom,
				Status:      status,
				ConfirmTime: confirmTime,
				ExtParam:    ext,
			}
			data := types.NewTxExtractData()
			data.Transaction = txObj
			result[fromRes.SourceKey] = append(result[fromRes.SourceKey], data)
		}

		// 监控接收方（to）：只有当 to 归属的 sourceKey 与 from 不同，才额外推一个“纯 output”记录。
		if toResExist && actualTo != "" {
			// 若 from/to 属于同一 sourceKey，则已在 fromRes 记录中包含 output，无需重复。
			if !fromRes.Exist || toRes.SourceKey != fromRes.SourceKey {
				txObj := &types.Transaction{
					TxID:        txid,
					AccountID:   toRes.SourceKey,
					Coin:        ethCoin,
					BlockHash:   blockHash,
					BlockHeight: blockHeight,
					Amount:      amountStr,
					Decimal:     bs.wm.SymbolDecimal(),
					// to-only 记录不记录手续费：gas 是顶层 tx.from 支付，所以 Fees 只保留在 payer（from 对应账户）的记录里。
					Fees:        "0",
					From:        []string{},
					To:          []string{actualTo + ":" + amountStr},
					Status:      status,
					ConfirmTime: confirmTime,
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

		// to 持币地址
		toParam := types.NewScanTargetParamForAddress(contractAddr, toToken)
		toRes := scanTargetFunc(toParam)

		if fromRes.Exist {
			txObj := &types.Transaction{
				TxID:        txid,
				AccountID:   fromRes.SourceKey,
				Coin:        tokenCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      tokenAmountStr,
				Decimal:     tokenDecimals,
				Fees:        "0",
				From:        []string{fromToken + ":" + tokenAmountStr},
				To:          []string{toToken + ":" + tokenAmountStr},
				Status:      status,
				ConfirmTime: confirmTime,
				// LogIndex 用于精确定位同一 tx 内多笔事件。
				LogIndex: int64(logIdx),
			}
			data := types.NewTxExtractData()
			data.Transaction = txObj
			result[fromRes.SourceKey] = append(result[fromRes.SourceKey], data)
		}

		if toRes.Exist {
			txObj := &types.Transaction{
				TxID:        txid,
				AccountID:   toRes.SourceKey,
				Coin:        tokenCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      tokenAmountStr,
				Decimal:     tokenDecimals,
				Fees:        "0",
				From:        []string{fromToken + ":" + tokenAmountStr},
				To:          []string{toToken + ":" + tokenAmountStr},
				Status:      status,
				ConfirmTime: confirmTime,
				LogIndex:    int64(logIdx),
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
			LogIndex:    int64(logIdx),
		}
	}

	return result, contractReceipts, nil
}
