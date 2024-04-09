package bstore

import (
	"crypto/sha256"
	"fmt"
	"sort"

	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"github.com/multiformats/go-multihash"
	"google.golang.org/protobuf/proto"
)

// MockBlock is similar to a Block.
//
// MockBlock is referred to by a number.  For example, we might represent a forked blockchain like this:
//
// 101 -> 102 -> 103 -> 104
//
//	\
//	 ---> 203 -> 204
//
// The numbers 101, 102, 103, 104, 203, 204, etc. are used to explain how the blocks relate to each other
// topologically.
//
// For example, if we're constructing block 203 in some variable mb, we would say mb.Previous = 102.
// These MockBlocks would be contained in a MockBlockTree, and the number-based Previous is translated to
// an actual block ID that obeys proper cryptographic constraints by ToBlockTree().
type MockBlock struct {
	Num      uint64
	Previous uint64

	ActiveData    []byte
	PassiveData   []byte
	SignatureData []byte

	Transactions []*protocol.Transaction

	Receipt []byte
}

// MockBlockTree tracks mock blocks by number.
type MockBlockTree struct {
	// MockBlock indexed by number
	ByNum map[uint64]*MockBlock
}

// BlockTree tracks blocks by number.
type BlockTree struct {
	// Block indexed by number
	ByNum map[uint64]*protocol.Block

	// Receipt indexed by number
	ReceiptByNum map[uint64][]byte

	// MockBlock by number
	Numbers []uint64
}

// NewMockBlock creates a new MockBlock object.
func NewMockBlock() *MockBlock {
	mb := MockBlock{
		Previous:      0,
		SignatureData: make([]byte, 0),

		Transactions: make([]*protocol.Transaction, 0),

		Receipt: make([]byte, 0),
	}
	return &mb
}

// GetEmptyBlockID computes the zero block ID (i.e. Previous of first block applied to genesis state)
func GetEmptyBlockID() []byte {
	vb := make([]byte, 32)
	mHashBuf, _ := multihash.EncodeName(vb, "sha2-256")
	return mHashBuf
}

// ComputeBlockID computes the block ID according to cryptographic constraints
func ComputeBlockID(block *protocol.Block) []byte {
	sHeader, _ := proto.Marshal(block.GetHeader())
	sDataToHash := sHeader

	hash := sha256.Sum256(sDataToHash)

	data, _ := multihash.EncodeName(hash[:], "sha256")
	return data
}

// ToBlockTree converts a MockBlockTree to a BlockTree
func ToBlockTree(mbt *MockBlockTree) *BlockTree {
	nums := make([]uint64, len(mbt.ByNum))
	i := 0
	for num := range mbt.ByNum {
		nums[i] = num
		i++
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	bt := BlockTree{
		ByNum:        make(map[uint64]*protocol.Block),
		ReceiptByNum: make(map[uint64][]byte),
	}
	for i = 0; i < len(nums); i++ {
		num := nums[i]
		mb := mbt.ByNum[num]
		b := protocol.Block{Header: &protocol.BlockHeader{}}
		if mb.Previous == 0 {
			b.Header.Previous = GetEmptyBlockID()
			b.Header.Height = 1
		} else {
			prevBlock := bt.ByNum[mb.Previous]
			b.Header.Previous = prevBlock.GetId()
			b.Header.Height = prevBlock.Header.Height + 1
		}

		b.Header.Timestamp = b.GetHeader().GetHeight()
		// TODO: Implement cryptographic constraints on signature and transactions
		b.Signature = mb.SignatureData
		b.Transactions = make([]*protocol.Transaction, 0)

		b.Id = ComputeBlockID(&b)
		//id, _ := json.Marshal(b.ID)
		//previd, _ := json.Marshal(b.Header.Previous)
		//fmt.Printf("Previous of %s is %s\n", id, previd)
		bt.ByNum[num] = &b
		bt.ReceiptByNum[num] = mb.Receipt
	}
	bt.Numbers = nums
	return &bt
}

// NewMockBlockTree creates a MockBlockTree from a tree specification
func NewMockBlockTree(tree [][]uint64) *MockBlockTree {
	mbt := MockBlockTree{
		ByNum: make(map[uint64]*MockBlock),
	}

	for i := 0; i < len(tree); i++ {
		var prev *MockBlock
		forkPoint := tree[i][0]
		if forkPoint == 0 {
			prev = nil
		} else {
			prev = mbt.ByNum[forkPoint]
		}
		for j := 1; j < len(tree[i]); j++ {
			num := tree[i][j]
			_, hasBlock := mbt.ByNum[num]
			if hasBlock {
				panic(fmt.Sprintf("Improperly specified tree, block %d specified multiple times\n", num))
			}
			mb := NewMockBlock()
			mb.Num = num
			if prev == nil {
				mb.Previous = 0
			} else {
				mb.Previous = prev.Num
			}
			mbt.ByNum[num] = mb
			prev = mb
		}
	}
	return &mbt
}
