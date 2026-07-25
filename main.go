package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"
)

var AdjustmentIntervalBlocks = 60
var TargetBlockTimeSecs = 10

type TxInput struct {
	prevTx     [32]byte
	txOutIndex uint16
}

func (txIn TxInput) bytes() []byte {
	var buf []byte

	buf, _ = binary.Append(buf, binary.LittleEndian, txIn.prevTx)
	buf = binary.LittleEndian.AppendUint16(buf, txIn.txOutIndex)

	return buf
}

type TxOutput struct {
	amount  uint64
	address []byte
}

func (txOut TxOutput) bytes() []byte {
	var buf []byte

	buf = binary.LittleEndian.AppendUint64(buf, (txOut.amount))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(txOut.address)))
	buf, _ = binary.Append(buf, binary.LittleEndian, txOut.address)

	return buf
}

type Transaction struct {
	inputs  []TxInput
	outputs []TxOutput
}

func (tx Transaction) bytes() []byte {
	var buf []byte

	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(tx.inputs)))
	for _, txIn := range tx.inputs {
		buf, _ = binary.Append(buf, binary.LittleEndian, txIn.bytes())
	}

	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(tx.outputs)))
	for _, txOut := range tx.outputs {
		buf, _ = binary.Append(buf, binary.LittleEndian, txOut.bytes())
	}

	return buf
}

func (tx Transaction) hash() [32]byte {
	return sha256.Sum256(tx.bytes())
}

type BlockHeader struct {
	prevHash [32]byte
	// Unix time
	timestamp  uint64
	merkleRoot [32]byte
}

type Block struct {
	BlockHeader
	transactions []Transaction
}

func (header BlockHeader) bytes() []byte {
	var buf []byte

	buf, _ = binary.Append(buf, binary.LittleEndian, header.prevHash)
	buf = binary.LittleEndian.AppendUint64(buf, header.timestamp)
	buf, _ = binary.Append(buf, binary.LittleEndian, header.merkleRoot)

	return buf
}

func (header BlockHeader) hash() [32]byte {
	return sha256.Sum256(header.bytes())
}

type Chain struct {
	currentTarget *big.Int
	blocks        []Block
}

func describeTarget(target *big.Int) string {
	bitLen := target.BitLen()
	leadingZeroBits := max(256-bitLen, 0)

	return fmt.Sprintf("bitLen=%d, approxLeadingZeroBits=%d", bitLen, leadingZeroBits)
}

func (chain *Chain) appendBlock(block Block) {
	chain.blocks = append(chain.blocks, block)

	if (len(chain.blocks) % AdjustmentIntervalBlocks) == 0 {
		chain.AdjustTarget()
	}
}

func (chain *Chain) AdjustTarget() *big.Int {
	if len(chain.blocks) < AdjustmentIntervalBlocks {
		return chain.currentTarget
	}

	windowStart := len(chain.blocks) - AdjustmentIntervalBlocks
	oldestTime := chain.blocks[windowStart].timestamp
	newestTime := chain.blocks[len(chain.blocks)-1].timestamp

	actualTime := newestTime - oldestTime
	if actualTime == 0 {
		actualTime = 1
	}

	expectedTime := uint64(AdjustmentIntervalBlocks * TargetBlockTimeSecs)

	target := new(big.Int).Set(chain.currentTarget)

	fmt.Printf("Old target: %s\n", describeTarget(target))

	target.Mul(target, big.NewInt(int64(actualTime)))
	target.Div(target, big.NewInt(int64(expectedTime)))

	if target.Sign() <= 0 {
		target.SetInt64(1)
	}

	chain.currentTarget = target

	fmt.Printf("New target: %s\n", describeTarget(target))

	return target
}

func (chain *Chain) mineUntilBlockFound() Block {
	var lastBlock = chain.blocks[len(chain.blocks)-1]

	var block Block

	var nonce uint64

	for {
		var nonceBuf []byte

		nonceBuf = binary.LittleEndian.AppendUint64(nonceBuf, nonce)

		var coinbaseTx = Transaction{
			inputs: []TxInput{},
			outputs: []TxOutput{
				{
					amount:  0,
					address: nonceBuf,
				},
				{
					amount:  1,
					address: nil,
				},
			},
		}

		block = Block{
			BlockHeader: BlockHeader{
				prevHash:   lastBlock.hash(),
				timestamp:  uint64(time.Now().Unix()),
				merkleRoot: coinbaseTx.hash(),
			},
			transactions: []Transaction{coinbaseTx},
		}

		var hash = block.hash()

		hashInt := new(big.Int).SetBytes(hash[:])
		targetInt := chain.currentTarget

		// fmt.Printf("h: %v, t: %v", hashInt, targetInt)

		if hashInt.Cmp(targetInt) < 0 {
			chain.appendBlock(block)

			return block
		}

		nonce += 1
	}
}

func (chain *Chain) mine() {
	for range 1000 {
		var newBlock = chain.mineUntilBlockFound()
		height := len(chain.blocks) - 1

		fmt.Printf("block %v: %v\n", height, newBlock.hash())
	}
}

func main() {
	var tx = Transaction{
		inputs: []TxInput{},
		outputs: []TxOutput{
			{
				amount:  1,
				address: nil,
			},
		},
	}

	var block = Block{
		BlockHeader: BlockHeader{
			prevHash:   [32]byte{},
			timestamp:  uint64(time.Now().Unix()),
			merkleRoot: tx.hash(),
		},
		transactions: []Transaction{tx},
	}

	var chain = Chain{
		currentTarget: new(big.Int).Lsh(big.NewInt(1), 256),
		blocks:        []Block{block},
	}

	chain.mine()
}
