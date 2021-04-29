package ethereumetl

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
)

type Transaction struct {
	Hash common.Hash
	Nonce uint64
	BlockHash common.Hash
	BlockNumber uint64
	TransactionIndex uint
	From common.Address
	To *common.Address
	Value *math.HexOrDecimal256
	Gas *big.Int
	GasPrice *big.Int
	Input []byte
	BlockTimestamp *big.Int
}