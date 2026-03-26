package scanner

// EthBlockScanner: 以太坊/EVM 区块扫描器
//
// 核心功能：
//   - 扫块：按高度拉取区块，提取主币/ERC20交易
//   - 提取：ExtractTransactionAndReceiptData 按 txid 精准提取交易数据
//   - 扫描：RunScanLoop 持续扫描满足确认数的安全区块
//
// 设计要点：
//   - 字段分离：FromAddr/FromAmt 与 ToAddr/ToAmt 一一对应
//   - 方向标记：TxAction(send/receive/internal/fee) 标识交易方向
//   - 输出索引：OutputIndex(-2=手续费, -1=主币, 0+=合约事件)
//   - 手续费独立：GAS 记录单独生成，避免在多条 Token 记录上重复计费
//   - 安全原则：未知精度代币/失败交易直接跳过，不猜测入账
//   - 缓存优化：tokenDecimalsCache、blockTimestampCache 减少重复 RPC

import (
	"context"
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

// EthBlockScanner 以太坊区块扫描器，实现 BlockScanner 接口。
// 嵌入 Base 复用任务调度，通过 WalletManager 进行 RPC 查询与交易提取。
type EthBlockScanner struct {
	*adaptscanner.Base
	// wm 提供 RPC 与 ERC20 元数据查询能力
	wm *manager.WalletManager

	// scannedHeight Run() 周期任务使用的内存游标（非持久化，仅用于自动追块场景）
	scannedHeight   uint64
	scannedHeightMu sync.RWMutex

	// scanLoopPause RunScanLoop 的暂停/恢复控制
	scanLoopMu      sync.Mutex
	scanLoopCond    *sync.Cond
	scanLoopPaused  bool
	scanLoopPauseCh chan struct{}

	// TxExtractConcurrency 单区块内并行提取交易的并发度（<=0 使用默认值）
	TxExtractConcurrency int

	// tokenDecimalsCache 合约精度缓存（>0=有效, 0=查询失败/非标准，调用方需跳过）
	tokenDecimalsCache      map[string]int32
	tokenDecimalsCacheMu    sync.RWMutex
	tokenDecimalsCacheMax   int      // 缓存上限，避免内存无限增长
	tokenDecimalsCacheOrder []string // FIFO 淘汰顺序

	// blockTimestampCache 区块时间戳缓存（FIFO 淘汰策略）
	blockTimestampCache      map[string]int64
	blockTimestampCacheMu    sync.RWMutex
	blockTimestampCacheMax   int
	blockTimestampCacheOrder []string

	// priorityScan 插队扫描状态
	priorityScanMu  sync.Mutex
	priorityHeights []uint64

	scanLoopConfirmations uint64                       // 确认数要求
	scanLoopHandleFunc    func(*types.BlockScanResult) // 扫描回调
}

// ScanBlockWithResult 按高度扫描并返回结果摘要。
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

	// 拉取完整区块
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

	// 逐笔提取交易（需设置 ScanTargetFunc）
	if bs.ScanTargetFunc != nil {
		txs := result.Get("transactions").Array()
		// TxTotal 在合并结果时统计

		// worker pool 并行提取（块内并行、块间串行）
		type txExtractOut struct {
			txHash           string
			extractData      []*types.ExtractDataItem
			contractReceipts []*types.ContractReceiptItem
			err              error
			matched          bool // 标记该交易是否命中监控地址（用于统计 TxTotal）
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

				// 使用超时上下文防止单个交易处理阻塞太久
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				type result struct {
					ed      []*types.ExtractDataItem
					cr      []*types.ContractReceiptItem
					matched bool
					err     error
				}
				resChan := make(chan result, 1)

				go func(node gjson.Result) {
					defer func() {
						if r := recover(); r != nil {
							resChan <- result{err: fmt.Errorf("panic: %v", r)}
						}
					}()
					ed, cr, matched, e := bs.extractTransactionAndReceiptDataFromBlockTx(node, hash, blockHeight, confirmTime, bs.ScanTargetFunc)
					resChan <- result{ed: ed, cr: cr, matched: matched, err: e}
				}(txNode)

				var outResult result
				select {
				case outResult = <-resChan:
					// 正常完成
				case <-ctx.Done():
					// 超时
					outResult = result{err: fmt.Errorf("extract transaction timeout: %s", txHash)}
				}
				cancel()

				outs <- txExtractOut{
					txHash:           txHash,
					extractData:      outResult.ed,
					contractReceipts: outResult.cr,
					err:              outResult.err,
					matched:          outResult.matched,
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

		// 单线程合并结果
		for out := range outs {
			if out.err != nil {
				res.TxFailed++
				if len(res.FailedTxIDs) < 20 {
					res.FailedTxIDs = append(res.FailedTxIDs, out.txHash)
				}
				continue
			}
			if out.matched {
				res.TxTotal++
			}

			if len(out.extractData) > 0 {
				for _, item := range out.extractData {
					if item == nil {
						continue
					}
					// 查找相同 SourceKey 的 item
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
		// 未设置扫描目标时只返回区块头信息
	}

	// Success: 区块获取成功且扫描流程完成（tx 提取失败时 Success=false）
	if res.TxFailed == 0 {
		res.Success = true
		return res, nil
	}
	res.Success = false
	res.ErrorReason = fmt.Sprintf("block scanned with %d tx extraction failures", res.TxFailed)
	return res, nil
}

// extractTransactionAndReceiptDataFromBlockTx 块内提取优化路径。
// tx 对象来自 eth_getBlockByNumber，只需调 eth_getTransactionReceipt 获取回执，confirmTime 由区块 timestamp 提供。
func (bs *EthBlockScanner) extractTransactionAndReceiptDataFromBlockTx(
	txNode gjson.Result,
	blockHash string,
	blockHeight uint64,
	confirmTime int64,
	scanTargetFunc adaptscanner.BlockScanTargetFunc,
) ([]*types.ExtractDataItem, []*types.ContractReceiptItem, bool, error) {
	if bs.wm == nil || bs.wm.Client == nil {
		return nil, nil, false, fmt.Errorf("wallet manager or rpc client is nil")
	}
	if bs.wm.Config == nil {
		return nil, nil, false, fmt.Errorf("wallet manager config is nil")
	}
	if scanTargetFunc == nil {
		return nil, nil, false, fmt.Errorf("scan target func is nil")
	}

	txid := txNode.Get("hash").String()
	if txid == "" {
		return nil, nil, false, fmt.Errorf("tx hash empty")
	}

	// 获取回执
	rcRes, err := bs.wm.Client.Call("eth_getTransactionReceipt", []interface{}{txid})
	if err != nil {
		return nil, nil, false, err
	}
	if !rcRes.Exists() || rcRes.Type == 0 {
		return nil, nil, false, fmt.Errorf("transaction receipt not found")
	}

	// 失败交易直接跳过
	status := types.TxStatusFail
	if statusHex := rcRes.Get("status").String(); statusHex != "" {
		st, err := hexutil.DecodeUint64(statusHex)
		if err != nil {
			return nil, nil, false, fmt.Errorf("decode status hex failed: %s, err: %v", statusHex, err)
		}
		if st == 1 {
			status = types.TxStatusSuccess
		}
	}
	if status == types.TxStatusFail {
		return nil, nil, false, nil
	}
	if blockHeight == 0 || blockHash == "" {
		return nil, nil, false, nil
	}

	// 基础字段来自区块 tx 对象
	from := strings.ToLower(txNode.Get("from").String())
	to := strings.ToLower(txNode.Get("to").String())
	valueHex := txNode.Get("value").String()

	amount := big.NewInt(0)
	if valueHex != "" && valueHex != "0x" {
		v, err := hexutil.DecodeBig(valueHex)
		if err != nil {
			return nil, nil, false, fmt.Errorf("decode value hex failed: %s, err: %v", valueHex, err)
		}
		amount = v
	}
	amountStr := util.BigIntToDecimal(amount, bs.wm.SymbolDecimal())

	// fee = gasUsed * effectiveGasPrice
	var fee *big.Int = big.NewInt(0)
	if gasUsedHex := rcRes.Get("gasUsed").String(); gasUsedHex != "" {
		gasUsed, err := hexutil.DecodeBig(gasUsedHex)
		if err != nil {
			fmt.Printf("[EthBlockScanner] decode gasUsed failed for tx %s: %s, err: %v\n", txid, gasUsedHex, err)
		} else {
			var gasPrice *big.Int
			// EIP-1559: 优先使用 receipt.effectiveGasPrice
			if gp := rcRes.Get("effectiveGasPrice").String(); gp != "" {
				decoded, err := hexutil.DecodeBig(gp)
				if err != nil {
					fmt.Printf("[EthBlockScanner] decode effectiveGasPrice failed for tx %s: %s, err: %v\n", txid, gp, err)
				} else {
					gasPrice = decoded
				}
			}
			// 非 EIP-1559 交易或获取失败时，回退到 tx.gasPrice
			if gasPrice == nil {
				if gp := txNode.Get("gasPrice").String(); gp != "" {
					decoded, err := hexutil.DecodeBig(gp)
					if err != nil {
						fmt.Printf("[EthBlockScanner] decode gasPrice failed for tx %s: %s, err: %v\n", txid, gp, err)
					} else {
						gasPrice = decoded
					}
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

// TokenMetadataFunc 现由外部在 Base 上统一注入（与 ScanTargetFunc 一致），此处仅依赖其存在而不再单独持有。
func NewBlockScanner(wm *manager.WalletManager) *EthBlockScanner {
	bs := &EthBlockScanner{
		Base:                    adaptscanner.NewBlockScannerBase(),
		wm:                      wm,
		TxExtractConcurrency:    10,
		tokenDecimalsCache:      make(map[string]int32),
		tokenDecimalsCacheMax:   1024,
		tokenDecimalsCacheOrder: make([]string, 0, 1024),
		blockTimestampCache:     make(map[string]int64),
		blockTimestampCacheMax:  1024,
	}
	bs.scanLoopCond = sync.NewCond(&bs.scanLoopMu)
	bs.scanLoopPauseCh = make(chan struct{})
	// Run() 周期任务自动追块（不含 Confirmations），推荐外部使用 RunScanLoop 自行维护游标
	bs.SetTask(bs.scanBlockTask)
	// 初始化游标为当前最新高度，避免从 0 开始全量扫历史
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

// scanBlockTask 自动追块任务：从 scannedHeight+1 扫到 latest，失败时停留在该高度重试。
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

// SetTxExtractConcurrency 设置单区块内并行提取交易的并发度（建议 1~100）。
func (bs *EthBlockScanner) SetTxExtractConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	if n > 100 {
		n = 100
	}
	bs.TxExtractConcurrency = n
}

// getTokenDecimals 返回合约代币精度（带缓存）。查询失败返回 (0, false)，调用方需跳过该代币。
func (bs *EthBlockScanner) getTokenDecimals(contractAddr string) (int32, bool) {
	if contractAddr == "" || bs.wm == nil {
		return 0, false
	}

	// 验证合约地址格式
	if !isValidAddress(contractAddr) {
		return 0, false
	}

	bs.tokenDecimalsCacheMu.RLock()
	d, ok := bs.tokenDecimalsCache[contractAddr]
	bs.tokenDecimalsCacheMu.RUnlock()
	if ok {
		if d > 0 {
			return d, true
		}
		return 0, false
	}

	// 1) 优先使用 TokenMetadataFunc
	if bs.TokenMetadataFunc != nil && bs.wm.Config != nil {
		if sc := bs.TokenMetadataFunc(bs.wm.Config.Symbol, contractAddr); sc != nil && sc.Decimals > 0 {
			decimals := int32(sc.Decimals)
			bs.setTokenDecimalsCache(contractAddr, decimals)
			return decimals, true
		}
	}

	// 2) 查询链上 ERC20 metadata
	_, _, dec, err := bs.wm.ERC20Metadata(contractAddr)
	if err != nil || dec == 0 {
		fmt.Printf("[EthBlockScanner] getTokenDecimals failed for contract %s: %v, decimals=%d\n", contractAddr, err, dec)
		bs.setTokenDecimalsCache(contractAddr, 0)
		return 0, false
	}

	decimals := int32(dec)
	bs.setTokenDecimalsCache(contractAddr, decimals)
	return decimals, true
}

// setTokenDecimalsCache 带 FIFO 淘汰策略的缓存写入
func (bs *EthBlockScanner) setTokenDecimalsCache(contractAddr string, decimals int32) {
	bs.tokenDecimalsCacheMu.Lock()
	defer bs.tokenDecimalsCacheMu.Unlock()

	if _, exists := bs.tokenDecimalsCache[contractAddr]; exists {
		bs.tokenDecimalsCache[contractAddr] = decimals
		// 更新访问顺序：移到队尾表示最近访问
		for i, addr := range bs.tokenDecimalsCacheOrder {
			if addr == contractAddr {
				// 移除当前位置
				bs.tokenDecimalsCacheOrder = append(bs.tokenDecimalsCacheOrder[:i], bs.tokenDecimalsCacheOrder[i+1:]...)
				break
			}
		}
		bs.tokenDecimalsCacheOrder = append(bs.tokenDecimalsCacheOrder, contractAddr)
		return
	}

	// FIFO 淘汰
	if bs.tokenDecimalsCacheMax > 0 && len(bs.tokenDecimalsCache) >= bs.tokenDecimalsCacheMax {
		if len(bs.tokenDecimalsCacheOrder) > 0 {
			oldest := bs.tokenDecimalsCacheOrder[0]
			delete(bs.tokenDecimalsCache, oldest)
			bs.tokenDecimalsCacheOrder = bs.tokenDecimalsCacheOrder[1:]
		}
	}

	bs.tokenDecimalsCache[contractAddr] = decimals
	bs.tokenDecimalsCacheOrder = append(bs.tokenDecimalsCacheOrder, contractAddr)
}

// isValidAddress 验证以太坊地址格式
func isValidAddress(addr string) bool {
	if addr == "" {
		return false
	}
	s := strings.TrimPrefix(strings.ToLower(addr), "0x")
	if len(s) != 40 {
		return false
	}
	_, err := hex.DecodeString(s)
	if err != nil {
		return false
	}
	// 检查零地址（燃烧地址）
	zeroAddr := "0000000000000000000000000000000000000000"
	if s == zeroAddr {
		return false
	}
	return true
}

// RunScanLoop 从外部指定的 StartHeight 开始（inclusive），持续按高度向上扫描，并始终只扫描已满足 Confirmations 的安全范围。
//   - 设定 safeTo = latest - Confirmations（latest 为链上最新高度）
//   - 每扫完一个区块高度就同步调用 HandleBlock（若 HandleBlock 为 nil 则忽略）。
func (bs *EthBlockScanner) RunScanLoop(params adaptscanner.ScanLoopParams) error {
	if bs.wm == nil || bs.wm.Client == nil {
		return fmt.Errorf("wallet manager or rpc client is nil")
	}
	if bs.wm.Config == nil {
		return fmt.Errorf("wallet manager config is nil")
	}
	if params.Interval <= 0 {
		params.Interval = 5 * time.Second
	}

	// 保存配置供插队扫描复用
	bs.scanLoopConfirmations = params.Confirmations
	bs.scanLoopHandleFunc = params.HandleBlock

	// cursor: 已处理的安全高度（safeTo），初始化为 StartHeight-1
	// 为了让第一次扫描命中 StartHeight，我们把 cursor 初始化到 StartHeight-1。
	// 当 StartHeight==0 时，cursor 保持为 0，第一次扫描从 0 开始（inclusive）。
	var cursor uint64
	if params.StartHeight == 0 {
		cursor = 0
	} else {
		cursor = params.StartHeight - 1
	}

	for {
		bs.blockIfPaused()

		// 优先处理插队扫描
		bs.processPriorityScan()

		latest, rpcErr := bs.GetGlobalMaxBlockHeightWithError()
		if rpcErr != nil || latest == 0 {
			errMsg := ""
			if rpcErr != nil {
				errMsg = rpcErr.Error()
			} else {
				errMsg = "cannot get latest block height (returned 0)"
			}
			// RPC 异常时回调通知
			if params.HandleBlock != nil {
				params.HandleBlock(&types.BlockScanResult{
					Symbol:           bs.wm.Config.Symbol,
					Success:          false,
					ErrorReason:      errMsg,
					ExtractData:      make([]*types.ExtractDataItem, 0),
					ContractReceipts: make([]*types.ContractReceiptItem, 0),
					FailedTxIDs:      make([]string, 0),
				})
			}
			// 等待 interval，支持 Pause 立即打断
			select {
			case <-time.After(params.Interval):
			case <-bs.getPauseCh():
			}
			continue
		}

		// 计算 safeTo = latest - Confirmations
		var safeTo uint64
		if latest > params.Confirmations {
			safeTo = latest - params.Confirmations
		} else {
			safeTo = 0
		}
		// safeTo=0 等待链增长
		if safeTo == 0 {
			select {
			case <-time.After(params.Interval):
			case <-bs.getPauseCh():
			}
			continue
		}

		// 扫描区间 [cursor+1..safeTo]：
		// - 扫描失败不退出 loop；
		// - 当某高度失败时，停留在该高度持续重试（不推进 cursor），并将失败结果回调给业务侧；
		// - 业务侧可根据 res.ErrorReason 决定告警、跳过高度或调整策略。
		scanFrom := cursor + 1

		// 无新高度时跳过内层扫描
		// 此时直接跳过内层扫描，仅等待 interval 后再次检查
		if scanFrom > safeTo {
			select {
			case <-time.After(params.Interval):
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

				if params.HandleBlock != nil {
					params.HandleBlock(res)
				}

				// 将 cursor 更新到 h-1：下一轮从 h 开始重试，避免重复扫更早高度。
				if h == 0 {
					cursor = 0
				} else if h-1 > cursor {
					cursor = h - 1
				}

				// 停留在该高度重试
				select {
				case <-time.After(params.Interval):
				case <-bs.getPauseCh():
				}
				break
			}

			if params.HandleBlock != nil {
				// network height（本轮 latest）用于业务侧观测进度与告警
				if res != nil {
					res.NetworkBlockHeight = latest
				}
				params.HandleBlock(res)
			}
		}

		// 将游标推进到当前轮的 safeTo。
		// 只有当本轮完整扫过至 safeTo 时才推进；若中途失败 break，则停留在失败高度重试。
		if completed {
			cursor = safeTo
		}
		select {
		case <-time.After(params.Interval):
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
		// RPC 异常时回调通知
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
		fmt.Printf("[EthBlockScanner] getBlockTimestamp RPC failed for %s: %v, using fallback %d\n", blockHash, err, fallback)
		return fallback
	}
	tsHex := blkRes.Get("timestamp").String()
	if tsHex == "" || tsHex == "0x" {
		fmt.Printf("[EthBlockScanner] getBlockTimestamp empty timestamp for %s, using fallback %d\n", blockHash, fallback)
		return fallback
	}
	tsUint, err := hexutil.DecodeUint64(tsHex)
	if err != nil || tsUint == 0 {
		fmt.Printf("[EthBlockScanner] getBlockTimestamp decode failed for %s: hex=%s err=%v, using fallback %d\n", blockHash, tsHex, err, fallback)
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
// 增加防御性长度检查防止 panic，同时检查零地址。
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
	// 检查零地址（燃烧地址）
	zeroAddr := "0000000000000000000000000000000000000000"
	if addrPart == zeroAddr {
		return ""
	}
	return "0x" + strings.ToLower(addrPart)
}

// ExtractTransactionAndReceiptData 按 txid 提取交易数据，按 SourceKey 聚合返回。
//
// scanTargetFunc: 优先使用 bs.ScanTargetFunc，未设置时使用传入参数。返回 nil 表示地址未监控。
//
// 提取规则：
//   - 过滤：交易必须上链且回执成功（status=1），pending/失败交易返回 nil
//   - 主币：value>0 时按 from/to 归属生成 send/receive/internal 记录，OutputIndex=-1
//   - 合约创建：to 为空时捕获 contractAddress，生成记录并标记 ExtParam["contract_creation"]
//   - 手续费：统一生成独立 GAS 记录（FeeType=gas, TxAction=fee, OutputIndex=-2），Fees=feeStr
//   - ERC20: 解析 Transfer 事件（标准32字节/非标准64字节 data），按 ScanTargetFunc 归属生成记录
//            decimals<=0 时跳过，不猜测默认精度；Token 记录 Fees="0"
//   - 返回: ExtractData 按 SourceKey 聚合，ContractReceipts 带 key(txid:contract:index) 便于定位

func (bs *EthBlockScanner) ExtractTransactionAndReceiptData(txid string, scanTargetFunc adaptscanner.BlockScanTargetFunc) ([]*types.ExtractDataItem, []*types.ContractReceiptItem, error) {
	if bs.wm == nil || bs.wm.Client == nil {
		return nil, nil, fmt.Errorf("wallet manager or rpc client is nil")
	}
	if bs.wm.Config == nil {
		return nil, nil, fmt.Errorf("wallet manager config is nil")
	}
	// 优先使用 bs.ScanTargetFunc，未设置时使用传入参数
	effectiveScanTargetFunc := bs.ScanTargetFunc
	if effectiveScanTargetFunc == nil {
		effectiveScanTargetFunc = scanTargetFunc
	}
	if effectiveScanTargetFunc == nil {
		return nil, nil, fmt.Errorf("scan target func is nil")
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
		h, err := hexutil.DecodeUint64(blockNumberHex)
		if err != nil {
			return nil, nil, fmt.Errorf("decode blockNumber hex failed: %s, err: %v", blockNumberHex, err)
		}
		blockHeight = h
	}

	// value 解析为主币金额
	amount := big.NewInt(0)
	if valueHex != "" && valueHex != "0x" {
		v, err := hexutil.DecodeBig(valueHex)
		if err != nil {
			return nil, nil, fmt.Errorf("decode value hex failed: %s, err: %v", valueHex, err)
		}
		amount = v
	}
	amountStr := util.BigIntToDecimal(amount, bs.wm.SymbolDecimal())

	// 手续费 = gasUsed * effectiveGasPrice (fall back 到 gasPrice)
	var fee *big.Int = big.NewInt(0)
	if gasUsedHex := rcRes.Get("gasUsed").String(); gasUsedHex != "" {
		gasUsed, err := hexutil.DecodeBig(gasUsedHex)
		if err != nil {
			return nil, nil, fmt.Errorf("decode gasUsed hex failed: %s, err: %v", gasUsedHex, err)
		}
		var gasPrice *big.Int
		// EIP-1559: 优先使用 receipt.effectiveGasPrice
		if gp := rcRes.Get("effectiveGasPrice").String(); gp != "" {
			decoded, err := hexutil.DecodeBig(gp)
			if err != nil {
				return nil, nil, fmt.Errorf("decode effectiveGasPrice hex failed: %s, err: %v", gp, err)
			}
			gasPrice = decoded
		}
		// 非 EIP-1559 交易或获取失败时，回退到 tx.gasPrice
		if gasPrice == nil {
			if gp := txRes.Get("gasPrice").String(); gp != "" {
				decoded, err := hexutil.DecodeBig(gp)
				if err != nil {
					return nil, nil, fmt.Errorf("decode gasPrice hex failed: %s, err: %v", gp, err)
				}
				gasPrice = decoded
			}
		}
		if gasPrice != nil {
			fee = new(big.Int).Mul(gasUsed, gasPrice)
		}
	}
	feeStr := util.BigIntToDecimal(fee, bs.wm.SymbolDecimal())

	// 默认视为失败，仅当回执 status 明确为 0x1 时视为成功（EIP-658）
	status := types.TxStatusFail
	if statusHex := rcRes.Get("status").String(); statusHex != "" {
		st, err := hexutil.DecodeUint64(statusHex)
		if err != nil {
			return nil, nil, fmt.Errorf("decode status hex failed: %s, err: %v", statusHex, err)
		}
		if st == 1 {
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

	// 区块时间查询失败时默认为 0
	confirmTime := bs.getBlockTimestamp(blockHash, 0)
	ed, cr, _, err := bs.extractTransactionAndReceiptDataFromParsed(txid, from, to, amount, amountStr, blockHash, blockHeight, confirmTime, fee, feeStr, status, rcRes, effectiveScanTargetFunc)
	return ed, cr, err
}

// extractTransactionAndReceiptDataFromParsed 生成交易单与合约回执的核心逻辑。
// scanTargetFunc 返回 nil 表示地址未监控（不生成记录）。
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
) ([]*types.ExtractDataItem, []*types.ContractReceiptItem, bool, error) {
	var extractData []*types.ExtractDataItem
	var contractReceipts []*types.ContractReceiptItem
	matched := false // 标记是否有地址命中监控

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
		if fromFeeRes != nil {
			matched = true
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
		var toRes *types.ScanTargetResult
		toResExist := false
		if actualTo != "" {
			toParam := types.NewScanTargetParamForAddress(bs.wm.Config.Symbol, actualTo)
			toRes = scanTargetFunc(toParam)
			toResExist = toRes != nil
			if toResExist {
				matched = true
			}
		}

		isContractCreation := actualTo == ""
		var ext map[string]string
		var createdAddr string
		if isContractCreation {
			// 获取创建的合约地址（从回执中）
			if rcRes != nil {
				createdAddr = rcRes.Get("contractAddress").String()
			}
			// 验证合约地址格式
			if createdAddr != "" && !isValidAddress(createdAddr) {
				fmt.Printf("[EthBlockScanner] invalid contract address created: %s\n", createdAddr)
				createdAddr = ""
			}
			ext = map[string]string{
				"contract_creation": "true",
				"contract_address":  createdAddr,
			}

			// 检查新创建的合约地址是否被监控（重要：合约创建时转入的 ETH 归属）
			if createdAddr != "" {
				createdParam := types.NewScanTargetParamForAddress(bs.wm.Config.Symbol, createdAddr)
				createdRes := scanTargetFunc(createdParam)
				if createdRes != nil {
					matched = true
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
		if fromRes != nil {
			matched = true
			// 判断是否为内部转账（双方属于同一账户）
			isInternal := toResExist && toRes != nil && toRes.SourceKey == fromRes.SourceKey
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
			if fromRes == nil || toRes.SourceKey != fromRes.SourceKey {
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
					b, err := hexutil.Decode(dataHex)
					if err != nil {
						fmt.Printf("[EthBlockScanner] skip ERC20 Transfer: decode data hex failed for %s: %v\n", dataHex, err)
						continue
					}
					if len(b) == 32 {
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
				b, err := hexutil.Decode(dataHex)
				if err != nil {
					fmt.Printf("[EthBlockScanner] skip ERC20 Transfer: decode data hex failed for %s: %v\n", dataHex, err)
					continue
				}
				if len(b) == 64 {
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

		// 使用 NewScanTargetParamForContract 让业务层能识别这是合约地址（ScanTargetTypeContractAddress）
		// 业务层可在 ScanTargetFunc 中通过 target.ScanTargetType 区分主币/合约，实现合约白名单过滤
		fromParam := types.NewScanTargetParamForContract(contractAddr, fromToken)
		fromRes := scanTargetFunc(fromParam)
		toParam := types.NewScanTargetParamForContract(contractAddr, toToken)
		toRes := scanTargetFunc(toParam)

		// 判断是否为内部转账（双方属于同一账户）
		isInternalToken := fromRes != nil && toRes != nil && fromRes.SourceKey == toRes.SourceKey

		if fromRes != nil {
			matched = true
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
		if toRes != nil && !isInternalToken {
			matched = true
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

	return extractData, contractReceipts, matched, nil
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
