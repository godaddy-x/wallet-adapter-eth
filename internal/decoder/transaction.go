// 本文件为 decoder 包中的 TransactionDecoder 实现（EVM 原生 + 可选 ERC20），
// 对应 github.com/godaddy-x/wallet-adapter 的 decoder.TransactionDecoder 接口。
package decoder

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	ethcom "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/godaddy-x/wallet-adapter-eth/internal/manager"
	"github.com/godaddy-x/wallet-adapter-eth/internal/models"
	"github.com/godaddy-x/wallet-adapter-eth/internal/util"
	"github.com/godaddy-x/wallet-adapter/decoder"
	"github.com/godaddy-x/wallet-adapter/types"
	"github.com/godaddy-x/wallet-adapter/wallet"
)

// EthTransactionDecoder 实现 decoder.TransactionDecoder（EVM 原生 + 可选 ERC20）
type EthTransactionDecoder struct {
	decoder.TransactionDecoderBase
	Wm *manager.WalletManager
}

// addressListPageSize 默认分页大小（在未根据总数自适应前的基准值）。
// 实际使用时会先通过 WalletDAI.GetAddressList(countQ=true) 拿到账户地址总数，
// 再结合 AddressLimit 动态计算每页 limit，避免一次性拉取全部地址或频繁扩容结果集。
const addressListPageSize = int64(50)

// calcAddressPageSize 根据总数与 AddressLimit 估算合适的分页大小：
// - 少量地址（<=100）时直接一次取完，减少 RPC/DB 往返；
// - 中等规模（<=1000、<=10000）按 100、500 分块；
// - 超大规模默认按 2000 分块，减少分页次数。
// 这样既兼顾了 ScanWrapper.GetAddressList(limit) 内部的容量预估，又避免单页过大导致的内存浪费。
func calcAddressPageSize(total, limit int64) int64 {
	if total <= 0 {
		return addressListPageSize
	}
	// 有 AddressLimit 时，最多只需要处理 limit 条
	effective := total
	if limit > 0 && limit < effective {
		effective = limit
	}

	switch {
	case effective <= 100:
		// 少量地址时直接一次性取完，避免多次往返
		return effective
	case effective <= 1000:
		return 100
	case effective <= 10000:
		return 500
	default:
		return 2000
	}
}

// NewTransactionDecoder 创建交易解码器
func NewTransactionDecoder(wm *manager.WalletManager) *EthTransactionDecoder {
	return &EthTransactionDecoder{Wm: wm}
}

// GetRawTransactionFeeRate 返回当前建议 gas price 与单位
func (d *EthTransactionDecoder) GetRawTransactionFeeRate(wrapper wallet.WalletDAI) (feeRate, unit string, err error) {
	price, err := d.Wm.GetGasPrice()
	if err != nil {
		return "", "", err
	}
	return util.BigIntToDecimal(price, d.Wm.SymbolDecimal()), "Gas", nil
}

// EstimateRawTransactionFee 根据 rawTx 估算手续费并写回 rawTx.Fees
func (d *EthTransactionDecoder) EstimateRawTransactionFee(wrapper wallet.WalletDAI, rawTx *types.RawTransaction) error {
	var toAddr, amountStr string
	for k, v := range rawTx.To {
		toAddr = k
		amountStr = v
		break
	}
	if rawTx.Account == nil {
		return types.Errorf(types.ErrCreateRawTransactionFailed, "account is nil")
	}
	addresses, _, err := wrapper.GetAddressList(false, 0, 1, "AccountID", rawTx.Account.AccountID)
	if err != nil || len(addresses) == 0 {
		return types.Errorf(types.ErrAddressNotFound, "no address for account")
	}
	from := addresses[0].Address
	amount, _ := util.StringToBigInt(amountStr, d.Wm.SymbolDecimal())
	feeInfo, err := d.Wm.GetTransactionFeeEstimated(from, toAddr, amount, nil)
	if err != nil {
		return types.ConvertError(err)
	}
	rawTx.Fees = util.BigIntToDecimal(feeInfo.Fee, d.Wm.SymbolDecimal())
	rawTx.FeeRate = util.BigIntToDecimal(feeInfo.GasPrice, d.Wm.SymbolDecimal())
	return nil
}

// CreateRawTransaction 创建原始交易（原生币或 ERC20）
func (d *EthTransactionDecoder) CreateRawTransaction(wrapper wallet.WalletDAI, rawTx *types.RawTransaction) error {
	if rawTx.Coin.IsContract {
		return d.createErc20RawTransaction(wrapper, rawTx)
	}
	return d.createSimpleRawTransaction(wrapper, rawTx, nil)
}

func (d *EthTransactionDecoder) createSimpleRawTransaction(wrapper wallet.WalletDAI, rawTx *types.RawTransaction, tmpNonce *uint64) error {
	if rawTx.Account == nil {
		return types.Errorf(types.ErrCreateRawTransactionFailed, "account is nil")
	}
	accountID := rawTx.Account.AccountID
	var toAddr, amountStr string
	for k, v := range rawTx.To {
		toAddr = k
		amountStr = v
		break
	}
	amount, err := util.StringToBigInt(amountStr, d.Wm.SymbolDecimal())
	if err != nil {
		return types.ConvertError(err)
	}

	// 分页查询地址，每页内检查余额，找到足够余额的地址即返回，避免一次拉取全部
	var lastID int64 = 0
	for {
		addresses, _, err := wrapper.GetAddressList(false, lastID, addressListPageSize, "AccountID", accountID)
		if err != nil {
			return types.NewError(types.ErrAddressNotFound, err.Error())
		}
		if len(addresses) == 0 {
			if lastID == 0 {
				return types.Errorf(types.ErrAddressNotFound, "account has no addresses")
			}
			break
		}
		var findBalance *models.AddrBalance
		for _, addr := range addresses {
			bal, err := d.Wm.GetAddrBalance(addr.Address, "pending")
			if err != nil {
				continue
			}
			feeInfo, err := d.Wm.GetTransactionFeeEstimated(addr.Address, toAddr, amount, nil)
			if err != nil {
				continue
			}
			total := new(big.Int).Add(amount, feeInfo.Fee)
			if bal.Cmp(total) >= 0 {
				findBalance = &models.AddrBalance{Address: addr.Address, Balance: bal}
				break
			}
		}
		if findBalance != nil {
			feeInfo, err := d.Wm.GetTransactionFeeEstimated(findBalance.Address, toAddr, amount, nil)
			if err != nil {
				return types.ConvertError(err)
			}
			if rawTx.FeeRate != "" {
				feeInfo.GasPrice, _ = util.StringToBigInt(rawTx.FeeRate, d.Wm.SymbolDecimal())
				feeInfo.CalcFee()
			}
			return d.buildRawTransaction(wrapper, rawTx, findBalance, feeInfo, "", tmpNonce)
		}
		if int64(len(addresses)) < addressListPageSize {
			break
		}
		lastID = getLastAddressID(addresses)
		if lastID <= 0 {
			break
		}
	}
	return types.Errorf(types.ErrInsufficientBalanceOfAccount, "insufficient balance for %s", amountStr)
}

// getLastAddressID 取分页结果中最后一条的 ID 用于下一页 lastID；空列表返回 0
func getLastAddressID(addresses []*types.Address) int64 {
	if len(addresses) == 0 {
		return 0
	}
	return addresses[len(addresses)-1].ID
}

// createErc20RawTransaction 创建 ERC20 转账原始交易：分页查地址，选代币余额与原生余额均足够的地址，编码 transfer(to, amount) 后建单
func (d *EthTransactionDecoder) createErc20RawTransaction(wrapper wallet.WalletDAI, rawTx *types.RawTransaction) error {
	if rawTx.Account == nil {
		return types.Errorf(types.ErrCreateRawTransactionFailed, "account is nil")
	}
	contractAddr := rawTx.Coin.Contract.Address
	if contractAddr == "" {
		return types.Errorf(types.ErrCreateRawTransactionFailed, "contract address is empty")
	}
	tokenDecimals := int32(rawTx.Coin.Contract.Decimals)
	var toAddr, amountStr string
	for k, v := range rawTx.To {
		toAddr = k
		amountStr = v
		break
	}
	amount, err := util.StringToBigInt(amountStr, tokenDecimals)
	if err != nil {
		return types.ConvertError(err)
	}

	lastID := int64(0)
	var errTokenBalance, errBalance string
	for {
		addresses, _, err := wrapper.GetAddressList(false, lastID, addressListPageSize, "AccountID", rawTx.Account.AccountID)
		if err != nil {
			return types.NewError(types.ErrAddressNotFound, err.Error())
		}
		if len(addresses) == 0 {
			if lastID == 0 {
				return types.Errorf(types.ErrAddressNotFound, "account has no addresses")
			}
			break
		}
		for _, addr := range addresses {
			tokenBal, err := d.Wm.ERC20BalanceOf(contractAddr, addr.Address)
			if err != nil || tokenBal == nil || tokenBal.Cmp(amount) < 0 {
				if err == nil && tokenBal != nil {
					errTokenBalance = "the token balance of all addresses is not enough"
				}
				continue
			}
			data, err := d.Wm.EncodeERC20Transfer(toAddr, amount)
			if err != nil {
				continue
			}
			feeInfo, err := d.Wm.GetTransactionFeeEstimated(addr.Address, contractAddr, nil, data)
			if err != nil {
				continue
			}
			if rawTx.FeeRate != "" {
				feeInfo.GasPrice, _ = util.StringToBigInt(rawTx.FeeRate, d.Wm.SymbolDecimal())
				feeInfo.CalcFee()
			}
			nativeBal, err := d.Wm.GetAddrBalance(addr.Address, "pending")
			if err != nil || nativeBal == nil || nativeBal.Cmp(feeInfo.Fee) < 0 {
				errBalance = "native balance not enough to pay gas"
				continue
			}
			ab := &models.AddrBalance{Address: addr.Address, Balance: nativeBal, TokenBalance: tokenBal}
			return d.buildRawTransaction(wrapper, rawTx, ab, feeInfo, hex.EncodeToString(data), nil)
		}
		if int64(len(addresses)) < addressListPageSize {
			break
		}
		lastID = getLastAddressID(addresses)
		if lastID <= 0 {
			break
		}
	}
	if errTokenBalance != "" {
		return types.Errorf(types.ErrInsufficientTokenBalanceOfAddress, "%s", errTokenBalance)
	}
	if errBalance != "" {
		return types.Errorf(types.ErrInsufficientFees, "%s", errBalance)
	}
	return types.Errorf(types.ErrInsufficientBalanceOfAccount, "insufficient token or native balance for %s", amountStr)
}

func (d *EthTransactionDecoder) buildRawTransaction(wrapper wallet.WalletDAI, rawTx *types.RawTransaction, ab *models.AddrBalance, fee *models.TxFeeInfo, callData string, tmpNonce *uint64) error {
	var toAddr, amountStr string
	for k, v := range rawTx.To {
		toAddr = k
		amountStr = v
		break
	}
	rawTx.TxFrom = []string{ab.Address + ":" + amountStr}
	rawTx.TxTo = []string{toAddr + ":" + amountStr}
	rawTx.FeeRate = util.BigIntToDecimal(fee.GasPrice, d.Wm.SymbolDecimal())
	rawTx.Fees = util.BigIntToDecimal(fee.Fee, d.Wm.SymbolDecimal())
	rawTx.TxAmount = amountStr

	addr, err := wrapper.GetAddress(ab.Address)
	if err != nil {
		return types.NewError(types.ErrAddressNotFound, err.Error())
	}
	var nonce uint64
	if tmpNonce != nil {
		nonce = *tmpNonce
	} else {
		if rawTx.ExtParam != nil {
			if n, ok := rawTx.ExtParam["nonce"]; ok {
				if parsed, perr := strconv.ParseUint(n, 10, 64); perr == nil {
					nonce = parsed
				}
			}
		}
		if nonce == 0 {
			nonce = d.Wm.GetAddressNonce(wrapper, ab.Address)
		}
	}

	signer := ethtypes.NewEIP155Signer(big.NewInt(int64(d.Wm.Config.ChainID)))
	gasLimit := fee.GasLimit.Uint64()

	var ethTx *ethtypes.Transaction
	if callData != "" {
		data, _ := hex.DecodeString(callData)
		ethTx = ethtypes.NewTransaction(nonce, ethcom.HexToAddress(Append0x(rawTx.Coin.Contract.Address)), big.NewInt(0), gasLimit, fee.GasPrice, data)
	} else {
		amount, _ := util.StringToBigInt(amountStr, d.Wm.SymbolDecimal())
		ethTx = ethtypes.NewTransaction(nonce, ethcom.HexToAddress(Append0x(toAddr)), amount, gasLimit, fee.GasPrice, nil)
	}

	rawHex, err := rlp.EncodeToBytes(ethTx)
	if err != nil {
		return types.ConvertError(err)
	}
	msgHash := signer.Hash(ethTx)

	if rawTx.Signatures == nil {
		rawTx.Signatures = make(map[string][]*types.KeySignature)
	}
	ks := &types.KeySignature{
		EccType: d.Wm.Config.CurveType,
		Nonce:   "0x" + strconv.FormatUint(nonce, 16),
		Address: addr,
		Message: hex.EncodeToString(msgHash[:]),
		RSV:     true,
	}
	rawTx.Signatures[rawTx.Account.AccountID] = []*types.KeySignature{ks}
	rawTx.RawHex = hex.EncodeToString(rawHex)
	rawTx.IsBuilt = true
	return nil
}

// VerifyRawTransaction 校验签名存在且格式正确（MPC 场景只做基本校验）
func (d *EthTransactionDecoder) VerifyRawTransaction(wrapper wallet.WalletDAI, rawTx *types.RawTransaction) error {
	if rawTx.Signatures == nil || len(rawTx.Signatures) == 0 {
		return types.Errorf(types.ErrVerifyRawTransactionFailed, "signature is empty")
	}
	accountID := rawTx.Account.AccountID
	if accountID == "" {
		return types.Errorf(types.ErrVerifyRawTransactionFailed, "account is nil")
	}
	sigs, ok := rawTx.Signatures[accountID]
	if !ok || len(sigs) == 0 {
		return types.Errorf(types.ErrVerifyRawTransactionFailed, "wallet signature not found")
	}
	sig := sigs[0]
	sigB, _ := hex.DecodeString(strings.TrimPrefix(sig.Signature, "0x"))
	msgB, _ := hex.DecodeString(strings.TrimPrefix(sig.Message, "0x"))
	pubB, _ := hex.DecodeString(strings.TrimPrefix(sig.Address.PublicKey, "0x"))
	if len(sigB) != 65 && len(sigB) != 64 {
		return types.Errorf(types.ErrVerifyRawTransactionFailed, "invalid signature length")
	}
	if len(msgB) != 32 {
		return types.Errorf(types.ErrVerifyRawTransactionFailed, "invalid message hash")
	}
	if len(pubB) != 65 {
		return types.Errorf(types.ErrVerifyRawTransactionFailed, "invalid public key")
	}
	return nil
}

// SubmitRawTransaction 合并 MPC 签名并广播
func (d *EthTransactionDecoder) SubmitRawTransaction(wrapper wallet.WalletDAI, rawTx *types.RawTransaction) (*types.Transaction, error) {
	if rawTx.Signatures == nil || len(rawTx.Signatures) == 0 {
		return nil, types.Errorf(types.ErrSubmitRawTransactionFailed, "signature is empty")
	}
	accountID := rawTx.Account.AccountID
	sigs, ok := rawTx.Signatures[accountID]
	if !ok || len(sigs) == 0 {
		return nil, types.Errorf(types.ErrSubmitRawTransactionFailed, "wallet signature not found")
	}
	sig := sigs[0]
	from := sig.Address.Address
	pubHex := sig.Address.PublicKey
	msgHex := sig.Message
	sigHex := sig.Signature

	rawHex, err := hex.DecodeString(rawTx.RawHex)
	if err != nil {
		return nil, types.ConvertError(err)
	}
	var ethTx ethtypes.Transaction
	if err := rlp.DecodeBytes(rawHex, &ethTx); err != nil {
		return nil, types.ConvertError(err)
	}

	ethSig, err := normalizeMPCSignatureForEthereum(pubHex, msgHex, sigHex)
	if err != nil {
		return nil, err
	}
	signer := ethtypes.NewEIP155Signer(big.NewInt(int64(d.Wm.Config.ChainID)))
	signedTx, err := ethTx.WithSignature(signer, ethcom.FromHex(ethSig))
	if err != nil {
		return nil, types.Errorf(types.ErrSubmitRawTransactionFailed, "with signature: %v", err)
	}
	encoded, err := rlp.EncodeToBytes(signedTx)
	if err != nil {
		return nil, types.ConvertError(err)
	}
	txid, err := d.Wm.SendRawTransaction(hexutil.Encode(encoded))
	if err != nil {
		d.Wm.UpdateAddressNonce(wrapper, from, 0)
		return nil, types.Errorf(types.ErrSubmitRawTransactionFailed, "broadcast: %v", err)
	}
	d.Wm.UpdateAddressNonce(wrapper, from, signedTx.Nonce()+1)
	rawTx.TxID = txid
	rawTx.IsSubmit = true

	return &types.Transaction{
		TxID:      txid,
		From:      rawTx.TxFrom,
		To:        rawTx.TxTo,
		Amount:    rawTx.TxAmount,
		Coin:      rawTx.Coin,
		AccountID: rawTx.Account.AccountID,
		Fees:      rawTx.Fees,
		Status:    types.TxStatusSuccess,
	}, nil
}

func normalizeMPCSignatureForEthereum(pubHex, msgHex, sigHex string) (string, error) {
	pubHex = strings.TrimPrefix(pubHex, "0x")
	msgHex = strings.TrimPrefix(msgHex, "0x")
	sigHex = strings.TrimPrefix(sigHex, "0x")
	pub, err := hex.DecodeString(pubHex)
	if err != nil || len(pub) != 65 || pub[0] != 0x04 {
		return "", fmt.Errorf("invalid public key")
	}
	msg, err := hex.DecodeString(msgHex)
	if err != nil || len(msg) != 32 {
		return "", fmt.Errorf("invalid message hash")
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil || len(sig) != 64 {
		return "", fmt.Errorf("invalid signature length")
	}
	pubKey, err := crypto.UnmarshalPubkey(pub)
	if err != nil {
		return "", err
	}
	r := new(big.Int).SetBytes(sig[0:32])
	s := new(big.Int).SetBytes(sig[32:64])
	N := crypto.S256().Params().N
	halfN := new(big.Int).Rsh(N, 1)
	if s.Cmp(halfN) > 0 {
		s = new(big.Int).Sub(N, s)
	}
	newR := ethcom.LeftPadBytes(r.Bytes(), 32)
	newS := ethcom.LeftPadBytes(s.Bytes(), 32)
	var recID int
	for v := 0; v < 2; v++ {
		testSig := append(append(newR, newS...), byte(v))
		recovered, err := crypto.Ecrecover(msg, testSig)
		if err != nil {
			continue
		}
		recoveredPub, _ := crypto.UnmarshalPubkey(recovered)
		if recoveredPub != nil && crypto.PubkeyToAddress(*recoveredPub) == crypto.PubkeyToAddress(*pubKey) {
			recID = v
			break
		}
	}
	finalSig := make([]byte, 65)
	copy(finalSig[0:32], newR)
	copy(finalSig[32:64], newS)
	finalSig[64] = byte(recID)
	return "0x" + hex.EncodeToString(finalSig), nil
}

// CreateSummaryRawTransactionWithError 汇总交易；分页查询地址，对满足条件的地址逐个构建汇总交易。
// - 原生币：直接基于地址原生余额汇总；
// - ERC20：基于合约代币余额汇总。
func (d *EthTransactionDecoder) CreateSummaryRawTransactionWithError(wrapper wallet.WalletDAI, sumRawTx *types.SummaryRawTransaction) ([]*types.RawTransactionWithError, error) {
	if sumRawTx.Coin.IsContract {
		return d.createErc20SummaryRawTransaction(wrapper, sumRawTx)
	}
	return d.createNativeSummaryRawTransaction(wrapper, sumRawTx)
}

// createNativeSummaryRawTransaction 原生币汇总交易；逻辑与原先 CreateSummaryRawTransactionWithError 保持一致。
// createNativeSummaryRawTransaction 原生币汇总交易：
// 1. 通过 countQ=true 的 GetAddressList 获取账户地址总数 total；
// 2. 基于 total 与 AddressLimit 计算每页 limit（calcAddressPageSize），尽量一次取完小批量地址；
// 3. 按 lastID 游标分页遍历地址，计算可汇总金额 = 余额 - 保留余额 - 手续费；
// 4. 为每个满足条件的地址构建 RawTransactionWithError，既返回成功交易，也保留单地址构建失败的错误。
// 设计目标：
// - 地址数量很大（上万、十万）时，减少分页次数与切片扩容；
// - 地址数量很少时，一次取完，避免多次 RPC/DB 往返；
// - AddressLimit 生效时，只处理前 N 条地址，最后一页按“剩余条数”精确设定 limit，避免过大预分配。
func (d *EthTransactionDecoder) createNativeSummaryRawTransaction(wrapper wallet.WalletDAI, sumRawTx *types.SummaryRawTransaction) ([]*types.RawTransactionWithError, error) {
	minTransfer, _ := util.StringToBigInt(sumRawTx.MinTransfer, d.Wm.SymbolDecimal())
	retainedBalance, _ := util.StringToBigInt(sumRawTx.RetainedBalance, d.Wm.SymbolDecimal())
	if minTransfer.Cmp(retainedBalance) < 0 {
		return nil, types.Errorf(types.ErrCreateRawTransactionFailed, "minTransfer must be >= retainedBalance")
	}

	var result []*types.RawTransactionWithError
	lastID := sumRawTx.AddressStartIndex

	// 首次 countQ 查询总数，用于估算分页大小，减少地址很多时的内存扩容与分页次数
	_, total, err := wrapper.GetAddressList(true, 0, 0, "AccountID", sumRawTx.Account.AccountID)
	if err != nil {
		return nil, err
	}
	if total == 0 {
		return nil, types.Errorf(types.ErrAddressNotFound, "no addresses")
	}
	pageSize := calcAddressPageSize(total, sumRawTx.AddressLimit)
	processedCount := int64(0)

	for {
		// 计算本页实际需要拉取的数量：
		// - remaining = total - 已处理条数；
		// - 若设置了 AddressLimit，则 remaining 还需受（AddressLimit - 已处理）约束；
		// - curLimit = min(pageSize, remaining)，确保最后一页不会按 pageSize 预分配过大容量。
		remaining := total - processedCount
		if sumRawTx.AddressLimit > 0 && sumRawTx.AddressLimit-processedCount < remaining {
			remaining = sumRawTx.AddressLimit - processedCount
		}
		if remaining <= 0 {
			return result, nil
		}
		curLimit := pageSize
		if remaining < curLimit {
			curLimit = remaining
		}

		addresses, _, err := wrapper.GetAddressList(false, lastID, curLimit, "AccountID", sumRawTx.Account.AccountID)
		if err != nil {
			return nil, err
		}
		if len(addresses) == 0 {
			if lastID == sumRawTx.AddressStartIndex {
				return nil, types.Errorf(types.ErrAddressNotFound, "no addresses")
			}
			break
		}
		for _, addr := range addresses {
			if sumRawTx.AddressLimit > 0 && processedCount >= sumRawTx.AddressLimit {
				return result, nil
			}
			processedCount++
			bal, err := d.Wm.GetAddrBalance(addr.Address, "pending")
			if err != nil || bal.Cmp(minTransfer) < 0 {
				continue
			}
			sumAmount := new(big.Int).Sub(bal, retainedBalance)
			feeInfo, err := d.Wm.GetTransactionFeeEstimated(addr.Address, sumRawTx.SummaryAddress, sumAmount, nil)
			if err != nil {
				result = append(result, &types.RawTransactionWithError{RawTx: nil, Error: types.ConvertError(err)})
				continue
			}
			sumAmount.Sub(sumAmount, feeInfo.Fee)
			if sumAmount.Sign() <= 0 {
				continue
			}
			rawTx := &types.RawTransaction{
				Coin:     sumRawTx.Coin,
				Account:  sumRawTx.Account,
				To:       map[string]string{sumRawTx.SummaryAddress: util.BigIntToDecimal(sumAmount, d.Wm.SymbolDecimal())},
				Required: 1,
			}
			ab := &models.AddrBalance{Address: addr.Address, Balance: bal}
			err = d.buildRawTransaction(wrapper, rawTx, ab, feeInfo, "", nil)
			result = append(result, &types.RawTransactionWithError{RawTx: rawTx, Error: types.ConvertError(err)})
		}
		if int64(len(addresses)) < pageSize {
			break
		}
		lastID = getLastAddressID(addresses)
		if lastID <= 0 {
			break
		}
	}
	return result, nil
}

// createErc20SummaryRawTransaction ERC20 汇总交易：遍历账户地址，按代币余额与主币余额构建汇总交易单。
// 核心流程：
// 1. 使用 countQ=true 的 GetAddressList 获取账户下地址总数 total，并结合 AddressLimit 计算分页大小；
// 2. 分页拉取地址列表，每页先批量调用 SmartContractDecoder.GetTokenBalanceByAddress 获取本页所有地址的代币余额；
// 3. 对于代币余额满足 minTransfer 且大于 0 的地址，按 (余额 - RetainedBalance) 计算可汇总数量；
// 4. 使用 EncodeERC20Transfer(summaryAddress, sumAmount) 生成 transfer 调用 data，并估算主币手续费；
// 5. 若主币余额足以支付手续费，则通过 buildRawTransaction 构造一笔 ERC20 汇总交易（to=合约地址，value=0，data=transfer）；
// 6. 每个地址的构建结果（成功/失败）都会以 RawTransactionWithError 形式返回，调用方可逐条处理。
// 这样在地址规模较大时仍能保持可控的内存与 RPC 次数，同时保证 ERC20 汇总语义清晰可靠。
func (d *EthTransactionDecoder) createErc20SummaryRawTransaction(wrapper wallet.WalletDAI, sumRawTx *types.SummaryRawTransaction) ([]*types.RawTransactionWithError, error) {
	if sumRawTx.Account == nil {
		return nil, types.Errorf(types.ErrCreateRawTransactionFailed, "account is nil")
	}
	tokenDecimals := int32(sumRawTx.Coin.Contract.Decimals)
	if tokenDecimals <= 0 {
		tokenDecimals = 18
	}
	minTransfer, _ := util.StringToBigInt(sumRawTx.MinTransfer, tokenDecimals)
	retainedBalance, _ := util.StringToBigInt(sumRawTx.RetainedBalance, tokenDecimals)
	if minTransfer.Cmp(retainedBalance) < 0 {
		return nil, types.Errorf(types.ErrCreateRawTransactionFailed, "minTransfer must be >= retainedBalance")
	}

	var result []*types.RawTransactionWithError
	lastID := sumRawTx.AddressStartIndex

	// 首次 countQ 查询总数，用于估算分页大小，减少地址很多时的内存扩容与分页次数
	_, total, err := wrapper.GetAddressList(true, 0, 0, "AccountID", sumRawTx.Account.AccountID)
	if err != nil {
		return nil, err
	}
	if total == 0 {
		return nil, types.Errorf(types.ErrAddressNotFound, "no addresses")
	}
	pageSize := calcAddressPageSize(total, sumRawTx.AddressLimit)
	processedCount := int64(0)

	contractDec := NewSmartContractDecoder(d.Wm)
	contract := sumRawTx.Coin.Contract
	contractAddr := contract.Address

	for {
		// 计算本页实际需要拉取的数量，避免最后一页仍按 pageSize 预分配过大容量
		remaining := total - processedCount
		if sumRawTx.AddressLimit > 0 && sumRawTx.AddressLimit-processedCount < remaining {
			remaining = sumRawTx.AddressLimit - processedCount
		}
		if remaining <= 0 {
			return result, nil
		}
		curLimit := pageSize
		if remaining < curLimit {
			curLimit = remaining
		}

		addresses, _, err := wrapper.GetAddressList(false, lastID, curLimit, "AccountID", sumRawTx.Account.AccountID)
		if err != nil {
			return nil, err
		}
		if len(addresses) == 0 {
			if lastID == sumRawTx.AddressStartIndex {
				return nil, types.Errorf(types.ErrAddressNotFound, "no addresses")
			}
			break
		}

		// 批量查询本页地址的代币余额
		searchAddrs := make([]string, 0, len(addresses))
		for _, a := range addresses {
			searchAddrs = append(searchAddrs, a.Address)
		}
		tokenBalances, err := contractDec.GetTokenBalanceByAddress(contract, searchAddrs...)
		if err != nil {
			return nil, err
		}

		for _, tb := range tokenBalances {
			if sumRawTx.AddressLimit > 0 && processedCount >= sumRawTx.AddressLimit {
				return result, nil
			}
			processedCount++

			addr := tb.Balance.Address
			tokenBalStr := tb.Balance.Balance
			tokenBal, _ := util.StringToBigInt(tokenBalStr, tokenDecimals)
			if tokenBal == nil || tokenBal.Cmp(minTransfer) < 0 || tokenBal.Sign() <= 0 {
				continue
			}

			// 计算汇总数量 = 余额 - 保留余额
			sumAmount := new(big.Int).Sub(tokenBal, retainedBalance)
			if sumAmount.Sign() <= 0 {
				continue
			}

			// 编码 ERC20 transfer(summaryAddress, sumAmount)
			callData, err := d.Wm.EncodeERC20Transfer(sumRawTx.SummaryAddress, sumAmount)
			if err != nil {
				result = append(result, &types.RawTransactionWithError{RawTx: nil, Error: types.ConvertError(err)})
				continue
			}

			// 估算主币手续费（to 为合约地址，value=0，data 为 transfer 调用）
			feeInfo, err := d.Wm.GetTransactionFeeEstimated(addr, contractAddr, nil, callData)
			if err != nil {
				result = append(result, &types.RawTransactionWithError{RawTx: nil, Error: types.ConvertError(err)})
				continue
			}
			if sumRawTx.FeeRate != "" {
				feeInfo.GasPrice, _ = util.StringToBigInt(sumRawTx.FeeRate, d.Wm.SymbolDecimal())
				feeInfo.CalcFee()
			}

			// 检查主币余额是否足够支付手续费
			nativeBal, err := d.Wm.GetAddrBalance(addr, "pending")
			if err != nil || nativeBal == nil || nativeBal.Cmp(feeInfo.Fee) < 0 {
				continue
			}

			sumAmountStr := util.BigIntToDecimal(sumAmount, tokenDecimals)
			rawTx := &types.RawTransaction{
				Coin:     sumRawTx.Coin,
				Account:  sumRawTx.Account,
				To:       map[string]string{sumRawTx.SummaryAddress: sumAmountStr},
				Required: 1,
			}
			ab := &models.AddrBalance{Address: addr, Balance: nativeBal}
			err = d.buildRawTransaction(wrapper, rawTx, ab, feeInfo, hex.EncodeToString(callData), nil)
			result = append(result, &types.RawTransactionWithError{RawTx: rawTx, Error: types.ConvertError(err)})
		}

		if int64(len(addresses)) < pageSize {
			break
		}
		lastID = getLastAddressID(addresses)
		if lastID <= 0 {
			break
		}
	}

	return result, nil
}
