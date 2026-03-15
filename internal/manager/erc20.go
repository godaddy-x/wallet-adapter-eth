package manager

import (
	"math/big"
	"strings"

	"github.com/blockchain/wallet-adapter-eth/internal/models"
	"github.com/blockchain/wallet-adapter-eth/internal/util"
	"github.com/ethereum/go-ethereum/accounts/abi"
	ethcom "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// ERC20 标准 transfer / balanceOf 的 ABI（仅保留需要的方法）
const erc20TransferBalanceOfABI = `[{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]`

var erc20ABI abi.ABI

func init() {
	var err error
	erc20ABI, err = abi.JSON(strings.NewReader(erc20TransferBalanceOfABI))
	if err != nil {
		panic("erc20 abi: " + err.Error())
	}
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
	return hexutil.DecodeBig(result)
}
