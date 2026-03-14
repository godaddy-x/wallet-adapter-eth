// 本文件为 decoder 包中的 TransactionDecoder 实现（EVM 原生 + 可选 ERC20），
// 对应 wallet-adapter 的 decoder.TransactionDecoder 接口。
package decoder

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/blockchain/wallet-adapter-eth/internal/manager"
	"github.com/blockchain/wallet-adapter-eth/internal/models"
	"github.com/blockchain/wallet-adapter-eth/internal/util"
	"github.com/blockchain/wallet-adapter/decoder"
	"github.com/blockchain/wallet-adapter/types"
	"github.com/blockchain/wallet-adapter/wallet"
	ethcom "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tidwall/gjson"
)

// EthTransactionDecoder 实现 decoder.TransactionDecoder（EVM 原生 + 可选 ERC20）
type EthTransactionDecoder struct {
	decoder.TransactionDecoderBase
	Wm *manager.WalletManager
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
	addresses, err := wrapper.GetAddressList(0, 1, "AccountID", rawTx.Account.AccountID)
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

// CreateRawTransaction 创建原始交易（仅原生币；ERC20 可后续扩展）
func (d *EthTransactionDecoder) CreateRawTransaction(wrapper wallet.WalletDAI, rawTx *types.RawTransaction) error {
	if rawTx.Coin.IsContract {
		return types.Errorf(types.ErrCreateRawTransactionFailed, "ERC20 not implemented in this adapter yet")
	}
	return d.createSimpleRawTransaction(wrapper, rawTx, nil)
}

func (d *EthTransactionDecoder) createSimpleRawTransaction(wrapper wallet.WalletDAI, rawTx *types.RawTransaction, tmpNonce *uint64) error {
	if rawTx.Account == nil {
		return types.Errorf(types.ErrCreateRawTransactionFailed, "account is nil")
	}
	accountID := rawTx.Account.AccountID
	addresses, err := wrapper.GetAddressList(0, -1, "AccountID", accountID)
	if err != nil {
		return types.NewError(types.ErrAddressNotFound, err.Error())
	}
	if len(addresses) == 0 {
		return types.Errorf(types.ErrAddressNotFound, "account has no addresses")
	}
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
	searchAddrs := make([]string, len(addresses))
	for i := range addresses {
		searchAddrs[i] = addresses[i].Address
	}
	var findBalance *models.AddrBalance
	for _, addr := range searchAddrs {
		bal, err := d.Wm.GetAddrBalance(addr, "pending")
		if err != nil {
			continue
		}
		feeInfo, err := d.Wm.GetTransactionFeeEstimated(addr, toAddr, amount, nil)
		if err != nil {
			continue
		}
		total := new(big.Int).Add(amount, feeInfo.Fee)
		if bal.Cmp(total) >= 0 {
			findBalance = &models.AddrBalance{Address: addr, Balance: bal}
			break
		}
	}
	if findBalance == nil {
		return types.Errorf(types.ErrInsufficientBalanceOfAccount, "insufficient balance for %s", amountStr)
	}
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
		if rawTx.ExtParam != "" {
			if n := gjson.Get(rawTx.ExtParam, "nonce"); n.Exists() {
				nonce = n.Uint()
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

// CreateSummaryRawTransactionWithError 汇总交易（仅原生币）
func (d *EthTransactionDecoder) CreateSummaryRawTransactionWithError(wrapper wallet.WalletDAI, sumRawTx *types.SummaryRawTransaction) ([]*types.RawTransactionWithError, error) {
	if sumRawTx.Coin.IsContract {
		return nil, types.Errorf(types.ErrCreateRawTransactionFailed, "ERC20 summary not implemented yet")
	}
	minTransfer, _ := util.StringToBigInt(sumRawTx.MinTransfer, d.Wm.SymbolDecimal())
	retainedBalance, _ := util.StringToBigInt(sumRawTx.RetainedBalance, d.Wm.SymbolDecimal())
	if minTransfer.Cmp(retainedBalance) < 0 {
		return nil, types.Errorf(types.ErrCreateRawTransactionFailed, "minTransfer must be >= retainedBalance")
	}
	addresses, err := wrapper.GetAddressList(int64(sumRawTx.AddressStartIndex), sumRawTx.AddressLimit, "AccountID", sumRawTx.Account.AccountID)
	if err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, types.Errorf(types.ErrAddressNotFound, "no addresses")
	}
	var result []*types.RawTransactionWithError
	for _, addr := range addresses {
		bal, err := d.Wm.GetAddrBalance(addr.Address, "pending")
		if err != nil || bal.Cmp(minTransfer) < 0 {
			continue
		}
		sumAmount := new(big.Int).Sub(bal, retainedBalance)
		sumAmount.Sub(sumAmount, big.NewInt(0))
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
	return result, nil
}
