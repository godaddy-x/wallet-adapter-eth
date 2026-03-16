package manager

import (
	"math/big"
	"strings"

	"github.com/godaddy-x/wallet-adapter-eth/internal/models"
	"github.com/godaddy-x/wallet-adapter-eth/internal/util"
	"github.com/ethereum/go-ethereum/accounts/abi"
	ethcom "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// ERC20 标准 transfer / balanceOf / name / symbol / decimals 的 ABI
const erc20ABIJSON = `[{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"}]`

var erc20ABI abi.ABI

func init() {
	var err error
	erc20ABI, err = abi.JSON(strings.NewReader(erc20ABIJSON))
	if err != nil {
		panic("erc20 abi: " + err.Error())
	}
}

// ERC20Metadata 查询 ERC20 合约的 name、symbol、decimals
func (wm *WalletManager) ERC20Metadata(contractAddr string) (name, symbol string, decimals uint8, err error) {
	contractAddr = util.Append0x(contractAddr)
	zeroAddr := ethcom.HexToAddress("0x0000000000000000000000000000000000000000")
	msg := models.CallMsg{From: zeroAddr, To: ethcom.HexToAddress(contractAddr), Data: nil, Value: big.NewInt(0)}

	unpackStr := func(method string) (string, error) {
		data, _ := erc20ABI.Pack(method)
		msg.Data = data
		result, err := wm.EthCall(msg, "latest")
		if err != nil || result == "" || result == "0x" {
			return "", err
		}
		if !strings.HasPrefix(result, "0x") {
			result = "0x" + result
		}
		out, decErr := hexutil.Decode(result)
		if decErr != nil {
			return "", decErr
		}
		unpacked, uErr := erc20ABI.Unpack(method, out)
		if uErr != nil || len(unpacked) == 0 {
			return "", uErr
		}
		if s, ok := unpacked[0].(string); ok {
			return s, nil
		}
		return "", nil
	}
	name, _ = unpackStr("name")
	symbol, _ = unpackStr("symbol")

	data, _ := erc20ABI.Pack("decimals")
	msg.Data = data
	result, err := wm.EthCall(msg, "latest")
	if err == nil && result != "" && result != "0x" {
		if !strings.HasPrefix(result, "0x") {
			result = "0x" + result
		}
		out, decErr := hexutil.Decode(result)
		if decErr == nil {
			unpacked, uErr := erc20ABI.Unpack("decimals", out)
			if uErr == nil && len(unpacked) > 0 {
				if d, ok := unpacked[0].(uint8); ok {
					decimals = d
				}
			}
		}
	}
	return name, symbol, decimals, nil
}

// EncodeERC20Transfer 编码 ERC20 transfer(to, value) 的 data
func (wm *WalletManager) EncodeERC20Transfer(to string, amount *big.Int) ([]byte, error) {
	toAddr := ethcom.HexToAddress(util.Append0x(to))
	return erc20ABI.Pack("transfer", toAddr, amount)
}

// ERC20BalanceOf 查询 ERC20 余额，contractAddr 为代币合约地址，accountAddr 为持币地址
func (wm *WalletManager) ERC20BalanceOf(contractAddr, accountAddr string) (*big.Int, error) {
	accountAddr = util.Append0x(accountAddr)
	contractAddr = util.Append0x(contractAddr)
	data, err := erc20ABI.Pack("balanceOf", ethcom.HexToAddress(accountAddr))
	if err != nil {
		return nil, err
	}
	msg := models.CallMsg{
		From:  ethcom.HexToAddress(accountAddr),
		To:    ethcom.HexToAddress(contractAddr),
		Data:  data,
		Value: big.NewInt(0),
	}
	result, err := wm.EthCall(msg, "latest")
	if err != nil {
		return nil, err
	}
	if result == "" || result == "0x" {
		return big.NewInt(0), nil
	}
	if !strings.HasPrefix(result, "0x") {
		result = "0x" + result
	}

	// balanceOf 返回的是 ABI 编码的 uint256，需要按 bytes 解析，而不是 JSON quantity（避免 leading zero 报错）
	out, decErr := hexutil.Decode(result)
	if decErr != nil {
		return nil, decErr
	}
	return new(big.Int).SetBytes(out), nil
}
