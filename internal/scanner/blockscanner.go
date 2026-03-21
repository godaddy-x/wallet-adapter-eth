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
//    - 主币：根据 tx.from / tx.to / value 构造 Transaction，FromAddr/FromAmt/ToAddr/ToAmt 字段分离设计
//      发送方地址与金额：FromAddr/FromAmt；接收方地址与金额：ToAddr/ToAmt（强制一一对应校验）
//    - Token：解析标准 ERC20 Transfer 事件（topics + data）构造 Transaction
//      Coin.Symbol 为主币符号，Coin.Contract 填充代币合约信息（地址、符号、名称、精度）
//      安全校验：严格检查 data 长度（32字节或64字节），防止恶意合约的金额解析攻击
//    - 手续费：为 tx.from 所属账户生成独立 GAS 记录（FeeType="gas", TxAction="fee"），避免在多条 Token 记录上重复记费
//      effectiveGasPrice 优先从 receipt 获取（EIP-1559），否则 fallback 到 tx.gasPrice
//    - 方向标记：通过 TxAction 字段标识交易方向（"send"=转出, "receive"=转入, "internal"=内部转账, "fee"=手续费）
//    - 输出索引：通过 OutputIndex 字段区分多笔记录（-2=手续费, -1=主币, 0+=合约事件索引）
//    - 合约创建：检测 to 为空的情况，捕获 contractAddress，并为新合约生成入账记录（如果被监控）
//    - 数据校验：通过 validateAddrAmtMatch 强制校验 FromAddr/FromAmt 和 ToAddr/ToAmt 长度一致
//    - 通过 ScanTargetFunc 仅对关心的地址/账户（AccountID=SourceKey）生成记录，并按 SourceKey 聚合
//
// 3. 验证与对账层（Verify-level）：
//    - VerifyTransactionByTxID：按 txid + minConfirmations 从链上二次复核（存在 / 成功 / 确认数），
//      并重新跑提取逻辑，返回可入账结果集 TxVerifyResult
//    - VerifyTransactionMatch：在 VerifyTransactionByTxID 基础上，与业务期望对象 TxVerifyExpected 严格比对
//      （主币比对 from/to/value，代币基于 txid+contractAddr+outputIndex 精确定位单条 Transfer）
//
// 4. 持续扫描循环（Cursor-level）：
//    - RunScanLoop：从指定起始高度 startHeight 开始（inclusive），持续扫描至 safeTo=latest-confirmations，
//      可选 windowSize 用于回填最近若干安全块以覆盖重组；每扫完一个高度将 BlockScanResult 同步回调给外部。
//
// 5. 性能与缓存：
//    - TxExtractConcurrency：控制单区块内按 txhash 并行提取的 goroutine 数量
//    - tokenDecimalsCache：按合约地址缓存 ERC20 decimals，>0 表示有效精度，0 表示“查询失败/非标准 ERC20”，调用方必须跳过
//    - blockTimestampCache：按 blockHash 缓存区块时间戳（秒），带上限与 FIFO 淘汰，避免重复 eth_getBlockByHash 与内存膨胀
//      若获取失败，fallback 值为 0（非当前时间），上层需处理 confirmTime<=0 的情况
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
	"github.com/tidwall/gjson"
)

// splitAddrAmount 辅助函数：从 "address:amount" 格式字符串中分离地址和金额
// 用于 decoder 层将旧格式 TxFrom/TxTo 转换为新的分离字段结构
// 注意：扫块器内部直接使用 FromAddr/FromAmt/ToAddr/ToAmt，无需调用此函数
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

	// priorityScan 插队扫描相关状态
	// 用于 RunScanLoop 中优先处理指定高度，暂停主线扫描
	priorityScanMu  sync.Mutex
	priorityHeights []uint64 // 插队高度列表

	// scanLoopConfig 保存 RunScanLoop 的配置
	// 用于插队扫描时复用相同的参数
	scanLoopConfirmations uint64                       // 确认数要求
	scanLoopHandleFunc    func(*types.BlockScanResult) // 回调函数
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
		ExtractData:      make([]*types.ExtractDataItem, 0),
		ContractReceipts: make([]*types.ContractReceiptItem, 0),
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

		// 代币：严格模式必须提供 outputIndex，用于唯一定位同一 tx 内的 Transfer 事件
		if tr.OutputIndex < 0 {
			out.Mismatches = append(out.Mismatches, "token outputIndex required for strict match")
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
		k := txid + ":" + contract + ":" + strconv.FormatInt(tr.OutputIndex, 10)
		r := findContractReceipt(chain.ContractReceipts, k)
		if r == nil {
			out.Mismatches = append(out.Mismatches, "token receipt not found by outputIndex")
			continue
		}
		// 事件精确定位：OutputIndex 必须与 expected 完全一致
		if r.OutputIndex != tr.OutputIndex {
			out.Mismatches = append(out.Mismatches, "token outputIndex mismatch")
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
		ExtractData:      make([]*types.ExtractDataItem, 0),
		ContractReceipts: make([]*types.ContractReceiptItem, 0),
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
	confirmTime := int64(timestamp)

	// 2) 逐笔提取交易（仅在设置 ScanTargetFunc 时进行；统计失败与提取数量，供外部观测）
	// 注意：ScanBlockWithResult 为“同步返回结果集”模式，不在此处做异步通知推送。
	if bs.ScanTargetFunc != nil {
		txs := result.Get("transactions").Array()
		res.TxTotal = uint64(len(txs))

		// worker pool 并行提取交易（块内并行、块间仍串行），提升吞吐并保持外部游标推进语义简单
		type txExtractOut struct {
			txHash           string
			extractData      []*types.ExtractDataItem
			contractReceipts []*types.ContractReceiptItem
			err              error
		}

		txNodes := make([]gjson.Result, 0, len(txs))
		for _, txNode := range txs {
			h := txNode.Get("hash").String()
			if h != "" {
				txNodes = append(txNodes, txNode)
			}
		}

		workers := bs.TxExtractConcurrency
		if workers <= 0 {
			workers = 10
		}
		if workers > len(txNodes) && len(txNodes) > 0 {
			workers = len(txNodes)
		}
		if workers == 0 {
			workers = 1
		}

		jobs := make(chan gjson.Result, workers)
		outs := make(chan txExtractOut, workers)

		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				for txNode := range jobs {
					txHash := txNode.Get("hash").String()
					ed, cr, e := bs.extractTransactionAndReceiptDataFromBlockTx(txNode, hash, blockHeight, confirmTime, bs.ScanTargetFunc)
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
			for _, txNode := range txNodes {
				jobs <- txNode
			}
			close(jobs)
			wg.Wait()
			close(outs)
		}()

		// 单线程合并结果，直接合并 slice，无 map 转换
		for out := range outs {
			if out.err != nil {
				res.TxFailed++
				if len(res.FailedTxIDs) < 20 {
					res.FailedTxIDs = append(res.FailedTxIDs, out.txHash)
				}
				continue
			}

			if len(out.extractData) > 0 {
				for _, item := range out.extractData {
					if item == nil {
						continue
					}
					// 查找是否已存在相同 SourceKey 的 item
					found := false
					for _, existing := range res.ExtractData {
						if existing.SourceKey == item.SourceKey {
							existing.Data = append(existing.Data, item.Data...)
							found = true
							break
						}
					}
					if !found {
						res.ExtractData = append(res.ExtractData, &types.ExtractDataItem{
							SourceKey: item.SourceKey,
							Data:      append([]*types.TxExtractData{}, item.Data...),
						})
					}
					res.ExtractedTxs += uint64(len(item.Data))
				}
			}
			if len(out.contractReceipts) > 0 {
				for _, item := range out.contractReceipts {
					if item == nil {
						continue
					}
					res.ContractReceipts = append(res.ContractReceipts, item)
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

// extractTransactionAndReceiptDataFromBlockTx 块内提取优化路径（用于 ScanBlockWithResult）：
// - tx 对象来自 eth_getBlockByNumber(height,true) 的 transactions 列表，已包含 from/to/value/gasPrice 等字段；
// - 因此这里不再调用 eth_getTransactionByHash，仅调用 eth_getTransactionReceipt 获取回执与 logs；
// - confirmTime 由区块 timestamp 提供，避免 per-tx 调用 eth_getBlockByHash。
func (bs *EthBlockScanner) extractTransactionAndReceiptDataFromBlockTx(
	txNode gjson.Result,
	blockHash string,
	blockHeight uint64,
	confirmTime int64,
	scanTargetFunc adaptscanner.BlockScanTargetFunc,
) ([]*types.ExtractDataItem, []*types.ContractReceiptItem, error) {
	if bs.wm == nil || bs.wm.Client == nil {
		return nil, nil, fmt.Errorf("wallet manager or rpc client is nil")
	}
	if bs.wm.Config == nil {
		return nil, nil, fmt.Errorf("wallet manager config is nil")
	}
	if scanTargetFunc == nil {
		return nil, nil, fmt.Errorf("scan target func is nil")
	}

	txid := txNode.Get("hash").String()
	if txid == "" {
		return nil, nil, fmt.Errorf("tx hash empty")
	}

	// receipt（必须）
	rcRes, err := bs.wm.Client.Call("eth_getTransactionReceipt", []interface{}{txid})
	if err != nil {
		return nil, nil, err
	}
	if !rcRes.Exists() || rcRes.Type == 0 {
		return nil, nil, fmt.Errorf("transaction receipt not found")
	}

	// status（失败直接跳过）
	status := types.TxStatusFail
	if statusHex := rcRes.Get("status").String(); statusHex != "" {
		if st, err := hexutil.DecodeUint64(statusHex); err == nil && st == 1 {
			status = types.TxStatusSuccess
		}
	}
	if status == types.TxStatusFail {
		return nil, nil, nil
	}
	if blockHeight == 0 || blockHash == "" {
		return nil, nil, nil
	}

	// 基础字段来自 block tx object
	from := strings.ToLower(txNode.Get("from").String())
	to := strings.ToLower(txNode.Get("to").String())
	valueHex := txNode.Get("value").String()

	amount := big.NewInt(0)
	if valueHex != "" && valueHex != "0x" {
		if v, err := hexutil.DecodeBig(valueHex); err == nil {
			amount = v
		}
	}
	amountStr := util.BigIntToDecimal(amount, bs.wm.SymbolDecimal())

	// fee = gasUsed * effectiveGasPrice（优先 receipt.effectiveGasPrice；fallback tx.gasPrice）
	var fee *big.Int = big.NewInt(0)
	if gasUsedHex := rcRes.Get("gasUsed").String(); gasUsedHex != "" {
		if gasUsed, err := hexutil.DecodeBig(gasUsedHex); err == nil {
			var gasPrice *big.Int
			if gp := rcRes.Get("effectiveGasPrice").String(); gp != "" {
				gasPrice, _ = hexutil.DecodeBig(gp)
			}
			if gasPrice == nil {
				if gp := txNode.Get("gasPrice").String(); gp != "" {
					gasPrice, _ = hexutil.DecodeBig(gp)
				}
			}
			if gasPrice != nil {
				fee = new(big.Int).Mul(gasUsed, gasPrice)
			}
		}
	}
	feeStr := util.BigIntToDecimal(fee, bs.wm.SymbolDecimal())

	return bs.extractTransactionAndReceiptDataFromParsed(txid, from, to, amount, amountStr, blockHash, blockHeight, confirmTime, fee, feeStr, status, rcRes, scanTargetFunc)
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

	// 保存 RunScanLoop 配置，供插队扫描复用
	bs.scanLoopConfirmations = confirmations
	bs.scanLoopHandleFunc = handleBlock

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

		// 优先处理插队扫描任务
		bs.processPriorityScan()

		latest, rpcErr := bs.GetGlobalMaxBlockHeightWithError()
		if rpcErr != nil || latest == 0 {
			errMsg := ""
			if rpcErr != nil {
				errMsg = rpcErr.Error()
			} else {
				errMsg = "cannot get latest block height (returned 0)"
			}
			// 通过回调通知业务层 RPC 连接异常
			if handleBlock != nil {
				handleBlock(&types.BlockScanResult{
					Symbol:           bs.wm.Config.Symbol,
					Success:          false,
					ErrorReason:      errMsg,
					ExtractData:      make([]*types.ExtractDataItem, 0),
					ContractReceipts: make([]*types.ContractReceiptItem, 0),
					FailedTxIDs:      make([]string, 0),
				})
			}
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

		// 优化：如果 scanFrom > safeTo，说明没有新高度需要扫描（已追平最新安全高度）
		// 此时直接跳过内层扫描，仅等待 interval 后再次检查
		if scanFrom > safeTo {
			select {
			case <-time.After(interval):
			case <-bs.getPauseCh():
			}
			continue
		}

		// 扫描区间 [scanFrom..safeTo]：
		// - 扫描失败不退出 loop；
		// - 当某高度失败时，停留在该高度持续重试（不推进 cursor），并将失败结果回调给业务侧；
		// - 业务侧可根据 res.ErrorReason 决定告警、跳过高度或调整策略。
		completed := true
		for h := scanFrom; h <= safeTo; h++ {
			bs.blockIfPaused()

			// 每处理一个高度前，先检查并处理插队任务（真正的插队逻辑）
			bs.processPriorityScan()

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
						ExtractData:      make([]*types.ExtractDataItem, 0),
						ContractReceipts: make([]*types.ContractReceiptItem, 0),
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

// ScanBlockPrioritize 插队扫描指定高度列表。
// 说明：
// - 将插队高度加入优先队列，RunScanLoop 会在主线扫描间隙优先处理这些高度；
// - 插队高度的扫描结果复用 RunScanLoop 的 handleBlock（如果 RunScanLoop 未运行则无回调）；
// - 插队扫描不影响 RunScanLoop 的主线 cursor 推进逻辑；
// - 插队高度按升序处理，且去重；
// - 严格要求所有传入的高度都满足 confirmations 要求（使用 RunScanLoop 的 confirmations）；
// - 如果 RunScanLoop 未运行，confirmations 默认为 0（此时只检查 height <= latest）；
// - 不满足条件的高度直接返回错误，调用方可根据错误信息调整高度后重试。
func (bs *EthBlockScanner) ScanBlockPrioritize(heights []uint64) error {
	if bs.wm == nil || bs.wm.Client == nil {
		return fmt.Errorf("wallet manager or rpc client is nil")
	}
	if len(heights) == 0 {
		return nil
	}

	// 获取链上最新高度，计算安全高度上限
	latest, rpcErr := bs.GetGlobalMaxBlockHeightWithError()
	if rpcErr != nil {
		return fmt.Errorf("cannot get latest block height: %w", rpcErr)
	}
	if latest == 0 {
		return fmt.Errorf("cannot get latest block height: returned 0")
	}

	// 使用 RunScanLoop 的 confirmations（如果 RunScanLoop 未运行，默认为 0）
	confirmations := bs.scanLoopConfirmations

	// 计算安全高度上限（满足 confirmations 要求的最大高度）
	var safeTo uint64
	if latest > confirmations {
		safeTo = latest - confirmations
	} else {
		safeTo = 0
	}

	// 检查所有高度是否都满足 confirmations 要求
	var invalidHeights []uint64
	for _, h := range heights {
		if h > safeTo {
			invalidHeights = append(invalidHeights, h)
		}
	}

	// 如果有不满足条件的高度，直接返回错误
	if len(invalidHeights) > 0 {
		return fmt.Errorf("heights %v exceed safe range (max allowed: %d, latest: %d, confirmations: %d)",
			invalidHeights, safeTo, latest, confirmations)
	}

	bs.priorityScanMu.Lock()
	defer bs.priorityScanMu.Unlock()

	// 将新高度加入队列（去重）
	heightMap := make(map[uint64]bool)
	for _, h := range bs.priorityHeights {
		heightMap[h] = true
	}
	added := 0
	for _, h := range heights {
		if !heightMap[h] {
			heightMap[h] = true
			bs.priorityHeights = append(bs.priorityHeights, h)
			added++
		}
	}

	// DEBUG: 打印队列状态
	fmt.Printf("[ScanBlockPrioritize] added %d heights, current queue: %v\n", added, bs.priorityHeights)

	// 排序，确保按高度升序处理
	// 使用简单冒泡排序（数据量小）
	for i := 0; i < len(bs.priorityHeights); i++ {
		for j := i + 1; j < len(bs.priorityHeights); j++ {
			if bs.priorityHeights[i] > bs.priorityHeights[j] {
				bs.priorityHeights[i], bs.priorityHeights[j] = bs.priorityHeights[j], bs.priorityHeights[i]
			}
		}
	}

	return nil
}

// processPriorityScan 处理插队扫描（由 RunScanLoop 调用）
// 返回处理的高度数量和是否继续处理
func (bs *EthBlockScanner) processPriorityScan() int {
	bs.priorityScanMu.Lock()
	if len(bs.priorityHeights) == 0 {
		bs.priorityScanMu.Unlock()
		return 0
	}

	// DEBUG: 打印插队队列内容
	fmt.Printf("[processPriorityScan] processing heights: %v\n", bs.priorityHeights)

	// 复制当前队列
	heights := make([]uint64, len(bs.priorityHeights))
	copy(heights, bs.priorityHeights)

	// 清空队列
	bs.priorityHeights = bs.priorityHeights[:0]
	bs.priorityScanMu.Unlock()

	// 获取 handleBlock 函数（可能为 nil，如果 RunScanLoop 未运行）
	handleFunc := bs.scanLoopHandleFunc

	// 获取链上最新高度，计算安全高度上限
	latest, rpcErr := bs.GetGlobalMaxBlockHeightWithError()
	if rpcErr != nil || latest == 0 {
		errMsg := ""
		if rpcErr != nil {
			errMsg = rpcErr.Error()
		} else {
			errMsg = "cannot get latest block height (returned 0)"
		}
		// 通过回调通知业务层 RPC 连接异常
		if handleFunc != nil {
			handleFunc(&types.BlockScanResult{
				Symbol:           bs.wm.Config.Symbol,
				Success:          false,
				ErrorReason:      "[PriorityScan] " + errMsg,
				ExtractData:      make([]*types.ExtractDataItem, 0),
				ContractReceipts: make([]*types.ContractReceiptItem, 0),
				FailedTxIDs:      make([]string, 0),
			})
		}
		return 0
	}
	confirmations := bs.scanLoopConfirmations
	var safeTo uint64
	if latest > confirmations {
		safeTo = latest - confirmations
	} else {
		safeTo = 0
	}

	processed := 0
	// 扫描所有插队高度（二次安全检查）
	for _, h := range heights {
		// 安全检查：仅处理满足 confirmations 要求的高度
		if h > safeTo {
			// 跳过不满足条件的高度，继续处理下一个
			continue
		}

		bs.blockIfPaused()

		res, err := bs.ScanBlockWithResult(h)
		if err != nil {
			// 构造失败结果
			if res == nil {
				res = &types.BlockScanResult{
					Symbol:           bs.wm.Config.Symbol,
					Height:           h,
					Success:          false,
					ErrorReason:      err.Error(),
					ExtractData:      make([]*types.ExtractDataItem, 0),
					ContractReceipts: make([]*types.ContractReceiptItem, 0),
					FailedTxIDs:      make([]string, 0),
					Once:             true, // 标记为插队扫描
				}
			}
		}

		if res != nil {
			res.NetworkBlockHeight = latest
			res.Once = true // 标记为插队扫描结果
			// 复用 RunScanLoop 的 handleBlock，如果设置了的话
			if handleFunc != nil {
				handleFunc(res)
			}
		}
		processed++
	}

	return processed
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
// 注意：此方法在 RPC 失败时返回 0，错误信息被静默丢弃；如需获取错误详情请使用 GetGlobalMaxBlockHeightWithError。
func (bs *EthBlockScanner) GetGlobalMaxBlockHeight() uint64 {
	h, _ := bs.GetGlobalMaxBlockHeightWithError()
	return h
}

// GetGlobalMaxBlockHeightWithError 获取链上最新区块高度，并返回可能的 RPC 错误。
// 该方法在节点连接断开或 RPC 调用失败时返回具体错误信息，供上层进行错误处理与告警。
func (bs *EthBlockScanner) GetGlobalMaxBlockHeightWithError() (uint64, error) {
	if bs.wm == nil || bs.wm.Client == nil {
		return 0, fmt.Errorf("wallet manager or rpc client is nil")
	}
	result, err := bs.wm.Client.Call("eth_blockNumber", nil)
	if err != nil {
		return 0, fmt.Errorf("rpc call eth_blockNumber failed: %w", err)
	}
	h, err := hexutil.DecodeUint64(result.String())
	if err != nil {
		return 0, fmt.Errorf("decode block number failed: %w", err)
	}
	return h, nil
}

// GetBalanceByAddress 查询指定地址的余额。
// 使用上层提供的 QueryBalancesConcurrent 辅助函数进行并发查询。
func (bs *EthBlockScanner) GetBalanceByAddress(address ...string) ([]*types.Balance, error) {
	if bs.wm == nil || bs.wm.Client == nil {
		return nil, fmt.Errorf("wallet manager or rpc client is nil")
	}

	if len(address) == 0 {
		return []*types.Balance{}, nil
	}

	// 定义单个地址查询函数，符合 BalanceQueryFunc 签名
	queryFunc := func(addr string) (confirmed, unconfirmed, total string, err error) {
		// 查询 confirmed 余额（latest）
		balanceConfirmed, err := bs.wm.GetAddrBalance(addr, "latest")
		if err != nil {
			return "", "", "", fmt.Errorf("get confirmed balance for %s failed: %w", addr, err)
		}

		// 查询 pending 余额（包含未确认交易）
		balanceAll, err := bs.wm.GetAddrBalance(addr, "pending")
		if err != nil {
			// 如果 pending 查询失败，使用 confirmed 余额作为 all
			balanceAll = balanceConfirmed
		}

		// 计算未确认余额
		balanceUnconfirmed := big.NewInt(0)
		balanceUnconfirmed.Sub(balanceAll, balanceConfirmed)

		confirmedStr := util.BigIntToDecimal(balanceConfirmed, bs.wm.SymbolDecimal())
		allStr := util.BigIntToDecimal(balanceAll, bs.wm.SymbolDecimal())
		unconfirmedStr := util.BigIntToDecimal(balanceUnconfirmed, bs.wm.SymbolDecimal())

		return confirmedStr, unconfirmedStr, allStr, nil
	}

	// 使用上层 Base 的并发查询辅助函数
	// 默认并发度 20，可通过 bs.TxExtractConcurrency 调整
	concurrency := bs.TxExtractConcurrency
	if concurrency <= 0 {
		concurrency = 20
	}

	return bs.Base.QueryBalancesConcurrent(bs.wm.Config.Symbol, address, queryFunc, concurrency)
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
// 不做 hex 解码校验：合规节点返回的 topic 均为合法十六进制，安全边界在节点层；但增加防御性长度检查防止panic。
func extractAddressFromTopic(topic string) string {
	s := strings.TrimSpace(topic)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		s = s[2:]
	}
	if len(s) != 64 {
		return ""
	}
	addrPart := s[24:] // 后 40 hex = 20 字节地址
	if len(addrPart) != 40 {
		return "" // 防御性检查：确保地址部分长度正确
	}
	return "0x" + strings.ToLower(addrPart)
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
//   - 若 from 与 to 归属同一 SourceKey：只生成一条记录（FromAddr=[from], FromAmt=[amount], ToAddr=[to], ToAmt=[amount]），TxAction="internal"
//   - 若归属不同 SourceKey：from 账户生成出账记录（TxAction="send"），to 账户生成入账记录（TxAction="receive"）
//   - 主币记录 Fees="0"，OutputIndex=-1，手续费统一通过 GAS 记录归集
//   - 若 to 为空（合约创建）：
//   - ExtParam["contract_creation"]="true"，ExtParam["contract_address"]=创建的合约地址
//   - 为发送方生成 TxAction="send" 的支出记录
//   - 如果创建的合约地址被监控，额外为其生成 TxAction="receive" 的入账记录（确保合约创建时转入的 ETH 被正确归属）
//
// 3. 手续费（GAS）：
//   - 手续费按 gasUsed * effectiveGasPrice（receipt 中优先，fallback 到 tx.gasPrice）计算为 feeWei，并按 SymbolDecimal() 格式化为 feeStr
//   - 无论是否包含主币转账，只要 tx 成功且 tx.from 命中 ScanTargetFunc，都会为该 AccountID 生成一条独立 GAS 记录：
//   - Coin = 原生币（ethCoin）
//   - Amount = "0"
//   - Fees   = feeStr
//   - FeeType = "gas"
//   - TxAction = "fee"
//   - OutputIndex = -2（与主币-1、合约事件0+区分）
//   - FromAddr=[from], FromAmt=[feeStr], ToAddr=[], ToAmt=[]
//   - 保证同一 AccountID+txid 最多仅一条记录包含非零 Fees，避免在多条业务记录上重复记费
//
// 4. ERC20 代币（Token）：
//   - 从回执 logs 中筛选标准 ERC20 Transfer 事件（topic0 固定为 Transfer(address,address,uint256)）
//   - Case1（标准）：topics[1]=from, topics[2]=to, data=32字节amount
//   - 安全校验：严格检查 data 长度必须为 32 字节，拒绝恶意合约的非标准事件
//   - Case2（非标准）：topics[1]=from, data=64字节（32字节to地址+32字节amount）
//   - 安全校验：严格检查 data 长度必须为 64 字节
//   - 通过 extractAddressFromTopic 从 32 字节 topic 中提取地址
//   - amount 按 uint256 从 data 解码，并通过 getTokenDecimals(contractAddr) 获取 decimals：
//   - 优先使用业务注入的 TokenMetadataFunc(symbol, contractAddr)
//   - 否则回退链上 ERC20Metadata(contractAddr)
//   - decimals<=0 或查询失败时跳过该事件（绝不猜测默认 18）
//   - 通过 ScanTargetFunc(contractAddr, fromToken/toToken) 判断代币归属的 AccountID：
//   - from 命中：在该 SourceKey 下生成一条 FromAddr=[fromToken], FromAmt=[amountStr], ToAddr=[toToken], ToAmt=[amountStr] 的记录，TxAction="send"（内部转账则为"internal"）
//   - to 命中：在该 SourceKey 下同样生成一条 FromAddr=[fromToken], FromAmt=[amountStr], ToAddr=[toToken], ToAmt=[amountStr] 的记录，TxAction="receive"
//     （两者 From/To 均完整填充，通过 TxAction 区分方向，便于按 AccountID 维度独立入账）
//   - Coin.Symbol = 主币符号（如 ETH）；Coin.IsContract = true
//   - Coin.Contract 填充：Symbol=代币符号, Address=合约地址, Name=代币名称, Decimals=代币精度
//   - Token 记录 Fees 始终为 "0"，手续费由 GAS 记录负责
//   - 每条 Transfer 事件同时生成一条 SmartContractReceipt，key=txid:contractAddr:outputIndex，用于 VerifyTransactionMatch 精确定位
//
// 5. 聚合维度：
//   - 返回值 ExtractData:  []*types.ExtractDataItem 按 SourceKey 聚合
//   - ContractReceipts: []*types.ContractReceiptItem 带 key 字段（如 txid:contractAddr:outputIndex）
//   - 上层可按 SourceKey 维度扫描账务，也可按 txid+outputIndex 做逐条严格比对
func (bs *EthBlockScanner) ExtractTransactionAndReceiptData(txid string, scanTargetFunc adaptscanner.BlockScanTargetFunc) ([]*types.ExtractDataItem, []*types.ContractReceiptItem, error) {
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
			// effectiveGasPrice is in receipt, not transaction (EIP-1559)
			if gp := rcRes.Get("effectiveGasPrice").String(); gp != "" {
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

	// 如果查询不到区块时间，默认设为 0（上层业务逻辑需要处理时间为 0 的情况）
	confirmTime := bs.getBlockTimestamp(blockHash, 0)
	return bs.extractTransactionAndReceiptDataFromParsed(txid, from, to, amount, amountStr, blockHash, blockHeight, confirmTime, fee, feeStr, status, rcRes, scanTargetFunc)
}

// extractTransactionAndReceiptDataFromParsed 统一的“生成结果集”逻辑（扫块/verify 复用）。
// 输入已解析好的 tx 基础字段 + receipt（用于 logs/status/gasUsed 等），输出交易单与合约回执集合（直接生成 slice，无 map 转换）。
func (bs *EthBlockScanner) extractTransactionAndReceiptDataFromParsed(
	txid string,
	from string,
	to string,
	amount *big.Int,
	amountStr string,
	blockHash string,
	blockHeight uint64,
	confirmTime int64,
	fee *big.Int,
	feeStr string,
	status string,
	rcRes *gjson.Result,
	scanTargetFunc adaptscanner.BlockScanTargetFunc,
) ([]*types.ExtractDataItem, []*types.ContractReceiptItem, error) {
	var extractData []*types.ExtractDataItem
	var contractReceipts []*types.ContractReceiptItem

	// 辅助函数：向 extractData 中添加数据，按 SourceKey 聚合
	appendExtractData := func(sourceKey string, data *types.TxExtractData) {
		// 严格校验：确保 Transaction 的地址和金额列表一一对应
		if data.Transaction != nil {
			if err := validateAddrAmtMatch(data.Transaction); err != nil {
				// 数据不一致，打印错误并跳过该记录
				fmt.Printf("[EthBlockScanner] validateAddrAmtMatch failed for %s: %v\n", data.Transaction.TxID, err)
				return
			}
		}
		for _, item := range extractData {
			if item.SourceKey == sourceKey {
				item.Data = append(item.Data, data)
				return
			}
		}
		extractData = append(extractData, &types.ExtractDataItem{
			SourceKey: sourceKey,
			Data:      []*types.TxExtractData{data},
		})
	}

	// 构造主币 Coin
	ethCoin := types.Coin{
		Symbol:     bs.wm.Config.Symbol,
		IsContract: false,
	}

	// 3.1 手续费独立 GAS 记录
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
				FromAddr:    []string{from},
				FromAmt:     []string{feeStr},
				ToAddr:      []string{},
				ToAmt:       []string{},
				Status:      status,
				ConfirmTime: confirmTime,
				FeeType:     "gas",
				TxAction:    "fee", // 手续费记录
				OutputIndex: -2,    // 手续费记录标记为 -2，与主币(-1)和合约事件(>=0)区分
				ExtParam:    nil,
			}
			data := types.NewTxExtractData()
			data.Transaction = feeTxObj
			appendExtractData(fromFeeRes.SourceKey, data)
		}
	}

	// 3. 主币转账
	if amount != nil && amount.Sign() > 0 && from != "" {
		actualTo := to
		var toRes types.ScanTargetResult
		toResExist := false
		if actualTo != "" {
			toParam := types.NewScanTargetParamForAddress(bs.wm.Config.Symbol, actualTo)
			toRes = scanTargetFunc(toParam)
			toResExist = toRes.Exist
		}

		isContractCreation := actualTo == ""
		var ext map[string]string
		var createdAddr string
		if isContractCreation {
			// 获取创建的合约地址（从回执中）
			if rcRes != nil {
				createdAddr = rcRes.Get("contractAddress").String()
			}
			ext = map[string]string{
				"contract_creation": "true",
				"contract_address":  createdAddr,
			}

			// 检查新创建的合约地址是否被监控（重要：合约创建时转入的 ETH 归属）
			if createdAddr != "" {
				createdParam := types.NewScanTargetParamForAddress(bs.wm.Config.Symbol, createdAddr)
				createdRes := scanTargetFunc(createdParam)
				if createdRes.Exist {
					// 为创建的合约地址生成 receive 记录（入账）
					txObj := &types.Transaction{
						TxID:        txid,
						AccountID:   createdRes.SourceKey,
						Coin:        ethCoin,
						BlockHash:   blockHash,
						BlockHeight: blockHeight,
						Amount:      amountStr,
						Decimal:     bs.wm.SymbolDecimal(),
						Fees:        "0",
						FromAddr:    []string{from},
						FromAmt:     []string{amountStr},
						ToAddr:      []string{createdAddr},
						ToAmt:       []string{amountStr},
						Status:      status,
						ConfirmTime: confirmTime,
						TxAction:    "receive", // 合约创建时的 ETH 入账
						OutputIndex: -1,
						ExtParam:    ext,
					}
					data := types.NewTxExtractData()
					data.Transaction = txObj
					appendExtractData(createdRes.SourceKey, data)
				}
			}
		}

		fromParam := types.NewScanTargetParamForAddress(bs.wm.Config.Symbol, from)
		fromRes := scanTargetFunc(fromParam)
		if fromRes.Exist {
			// 判断是否为内部转账（双方属于同一账户）
			isInternal := toResExist && toRes.SourceKey == fromRes.SourceKey
			txAction := "send"
			if isInternal {
				txAction = "internal"
			}
			txObj := &types.Transaction{
				TxID:        txid,
				AccountID:   fromRes.SourceKey,
				Coin:        ethCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      amountStr,
				Decimal:     bs.wm.SymbolDecimal(),
				Fees:        "0",
				FromAddr:    []string{from},
				FromAmt:     []string{amountStr},
				ToAddr:      []string{actualTo},
				ToAmt:       []string{amountStr},
				Status:      status,
				ConfirmTime: confirmTime,
				TxAction:    txAction, // 转出或内部转账
				OutputIndex: -1,       // 主币转账记录标记为 -1，与手续费(-2)和合约事件(>=0)区分
				ExtParam:    ext,
			}
			data := types.NewTxExtractData()
			data.Transaction = txObj
			appendExtractData(fromRes.SourceKey, data)
		}

		if toResExist && actualTo != "" {
			if !fromRes.Exist || toRes.SourceKey != fromRes.SourceKey {
				txObj := &types.Transaction{
					TxID:        txid,
					AccountID:   toRes.SourceKey,
					Coin:        ethCoin,
					BlockHash:   blockHash,
					BlockHeight: blockHeight,
					Amount:      amountStr,
					Decimal:     bs.wm.SymbolDecimal(),
					Fees:        "0",
					FromAddr:    []string{from},
					FromAmt:     []string{amountStr},
					ToAddr:      []string{actualTo},
					ToAmt:       []string{amountStr},
					Status:      status,
					ConfirmTime: confirmTime,
					TxAction:    "receive", // 转入
					OutputIndex: -1,        // 主币转账记录标记为 -1，与手续费(-2)和合约事件(>=0)区分
					ExtParam:    ext,
				}
				data := types.NewTxExtractData()
				data.Transaction = txObj
				appendExtractData(toRes.SourceKey, data)
			}
		}
	}

	// 4. ERC20 Transfer logs
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

		// 标准 ERC-20 Transfer 格式：topics[1]=from, topics[2]=to, data=32字节amount
		if len(topics) >= 3 {
			fromToken = extractAddressFromTopic(topics[1].String())
			toToken = extractAddressFromTopic(topics[2].String())
			if fromToken != "" && toToken != "" {
				if dataHex != "" && dataHex != "0x" {
					// 安全校验：严格检查 data 长度必须为 32 字节，防止恶意合约攻击
					if b, err := hexutil.Decode(dataHex); err == nil && len(b) == 32 {
						tokenAmount = new(big.Int).SetBytes(b)
					}
				}
			}
		} else if len(topics) >= 2 {
			fromToken = extractAddressFromTopic(topics[1].String())
			if fromToken == "" {
				continue
			}
			if dataHex != "" && dataHex != "0x" {
				// 非标准格式：data 应包含 32 字节 to 地址 + 32 字节 amount = 64 字节
				if b, err := hexutil.Decode(dataHex); err == nil && len(b) == 64 {
					addrBytes := b[12:32]
					if len(addrBytes) == 20 {
						toToken = "0x" + strings.ToLower(hex.EncodeToString(addrBytes))
					}
					tokenAmount = new(big.Int).SetBytes(b[32:64])
				}
			}
		}

		if tokenAmount == nil || tokenAmount.Sign() <= 0 || fromToken == "" || toToken == "" {
			continue
		}

		tokenDecimals, ok := bs.getTokenDecimals(contractAddr)
		if !ok || tokenDecimals <= 0 {
			fmt.Printf("[EthBlockScanner] skip ERC20 Transfer for contract %s due to unknown decimals\n", contractAddr)
			continue
		}
		tokenAmountStr := util.BigIntToDecimal(tokenAmount, tokenDecimals)

		tokenCoin := types.Coin{
			Symbol:     bs.wm.Config.Symbol,
			IsContract: true,
			Contract: types.SmartContract{
				Symbol:   "",
				Address:  contractAddr,
				Token:    "",
				Protocol: "",
				Name:     "",
				Decimals: uint64(tokenDecimals),
			},
		}

		fromParam := types.NewScanTargetParamForAddress(contractAddr, fromToken)
		fromRes := scanTargetFunc(fromParam)
		toParam := types.NewScanTargetParamForAddress(contractAddr, toToken)
		toRes := scanTargetFunc(toParam)

		// 判断是否为内部转账（双方属于同一账户）
		isInternalToken := fromRes.Exist && toRes.Exist && fromRes.SourceKey == toRes.SourceKey

		if fromRes.Exist {
			txAction := "send"
			if isInternalToken {
				txAction = "internal"
			}
			txObj := &types.Transaction{
				TxID:        txid,
				AccountID:   fromRes.SourceKey,
				Coin:        tokenCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      tokenAmountStr,
				Decimal:     tokenDecimals,
				Fees:        "0",
				FromAddr:    []string{fromToken},
				FromAmt:     []string{tokenAmountStr},
				ToAddr:      []string{toToken},
				ToAmt:       []string{tokenAmountStr},
				Status:      status,
				ConfirmTime: confirmTime,
				TxAction:    txAction, // 转出或内部转账
				OutputIndex: int64(logIdx),
			}
			data := types.NewTxExtractData()
			data.Transaction = txObj
			appendExtractData(fromRes.SourceKey, data)
		}
		if toRes.Exist && !isInternalToken {
			txObj := &types.Transaction{
				TxID:        txid,
				AccountID:   toRes.SourceKey,
				Coin:        tokenCoin,
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Amount:      tokenAmountStr,
				Decimal:     tokenDecimals,
				Fees:        "0",
				FromAddr:    []string{fromToken},
				FromAmt:     []string{tokenAmountStr},
				ToAddr:      []string{toToken},
				ToAmt:       []string{tokenAmountStr},
				Status:      status,
				ConfirmTime: confirmTime,
				TxAction:    "receive", // 转入
				OutputIndex: int64(logIdx),
			}
			data := types.NewTxExtractData()
			data.Transaction = txObj
			appendExtractData(toRes.SourceKey, data)
		}

		receiptKey := txid + ":" + contractAddr + ":" + strconv.Itoa(logIdx)
		contractReceipts = append(contractReceipts, &types.ContractReceiptItem{
			Key: receiptKey,
			Receipt: &types.SmartContractReceipt{
				Coin:        tokenCoin,
				TxID:        txid,
				From:        fromToken,
				To:          toToken,
				Value:       tokenAmountStr,
				Fees:        "0",
				BlockHash:   blockHash,
				BlockHeight: blockHeight,
				Status:      status,
				OutputIndex: int64(logIdx),
			},
		})
	}

	return extractData, contractReceipts, nil
}

// validateAddrAmtMatch 校验地址和金额列表的长度匹配性
// 确保 FromAddr/FromAmt 和 ToAddr/ToAmt 一一对应，避免数据不一致
func validateAddrAmtMatch(tx *types.Transaction) error {
	if len(tx.FromAddr) != len(tx.FromAmt) {
		return fmt.Errorf("FromAddr/FromAmt length mismatch: %d vs %d", len(tx.FromAddr), len(tx.FromAmt))
	}
	if len(tx.ToAddr) != len(tx.ToAmt) {
		return fmt.Errorf("ToAddr/ToAmt length mismatch: %d vs %d", len(tx.ToAddr), len(tx.ToAmt))
	}
	return nil
}

// findContractReceipt 在 ContractReceipts slice 中查找指定 key 的 receipt
func findContractReceipt(items []*types.ContractReceiptItem, key string) *types.SmartContractReceipt {
	for _, item := range items {
		if item == nil {
			continue
		}
		// 支持完整匹配或后缀匹配（兼容 txid:contract:outputIndex 和 contract:outputIndex 两种格式）
		if item.Key == key || strings.HasSuffix(item.Key, key) || strings.HasSuffix(key, item.Key) {
			return item.Receipt
		}
	}
	return nil
}
