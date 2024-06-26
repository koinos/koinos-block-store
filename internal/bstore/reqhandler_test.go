package bstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/multiformats/go-multihash"

	log "github.com/koinos/koinos-log-golang/v2"
	"github.com/koinos/koinos-proto-golang/v2/koinos"
	"github.com/koinos/koinos-proto-golang/v2/koinos/rpc/block_store"
)

const (
	MapBackendType    = 0
	BadgerBackendType = 1
)

var backendTypes = [...]int{MapBackendType, BadgerBackendType}

func NewBackend(backendType int) BlockStoreBackend {
	var backend BlockStoreBackend
	switch backendType {
	case MapBackendType:
		backend = NewMapBackend()
	case BadgerBackendType:
		dirname, err := os.MkdirTemp(os.TempDir(), "bstore-test-*")
		if err != nil {
			panic("unable to create temp directory")
		}
		opts := badger.DefaultOptions(dirname)
		backend, _ = NewBadgerBackend(opts)
	default:
		panic("unknown backend type")
	}
	return backend
}

func CloseBackend(b interface{}) {
	switch t := b.(type) {
	case *MapBackend:
		break
	case *BadgerBackend:
		t.Close()
	default:
		panic("unknown backend type")
	}
}

func SliceEqual(a []uint64, b []uint64) bool {
	n := len(a)
	if len(b) != n {
		return false
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGetPreviousHeights(t *testing.T) {
	testCases := [][][]uint64{
		{{0}, {}},
		{{1}, {0}},
		{{2}, {1, 0}},
		{{3}, {2}},
		{{4}, {3, 2, 0}},
		{{5}, {4}},
		{{6}, {5, 4}},
		{{7}, {6}},
		{{8}, {7, 6, 4, 0}},
		{{9}, {8}},
		{{10}, {9, 8}},
		{{11}, {10}},
		{{12}, {11, 10, 8}},
		{{13}, {12}},
		{{14}, {13, 12}},
		{{15}, {14}},
		{{16}, {15, 14, 12, 8, 0}},
		{{17}, {16}},
	}

	for i := 0; i < len(testCases); i++ {
		x := testCases[i][0][0]
		yRef := testCases[i][1]
		yTest := getPreviousHeights(x)

		if !SliceEqual(yRef, yTest) {
			t.Errorf("Testing %d, expected %v, got %v", x, yRef, yTest)
		}
	}
}

// GetNonExistentBlockID returns a made-up block ID that shouldn't correspond to any actual block.
func GetNonExistentBlockID(num uint64) []byte {
	dataBytes := make([]byte, binary.MaxVarintLen64)
	count := binary.PutUvarint(dataBytes, num)

	hash := sha256.Sum256(dataBytes[:count])

	mHashBuf, _ := multihash.EncodeName(hash[:], "sha2-256")
	return mHashBuf
}

func BuildTestTree(t *testing.T, handler *RequestHandler, bt *BlockTree) {
	nonExistentBlockID := GetNonExistentBlockID(999)

	for _, num := range bt.Numbers {

		addReq := &block_store.AddBlockRequest{BlockToAdd: bt.ByNum[num]}

		iReq := block_store.BlockStoreRequest_AddBlock{AddBlock: addReq}
		genericReq := &block_store.BlockStoreRequest{Request: &iReq}

		_, err := json.Marshal(genericReq)
		if err != nil {
			t.Error("Could not marshal JSON", err)
		}
		// fmt.Printf("Request: %s\n", j)

		result := handler.HandleRequest(genericReq)
		if result == nil {
			t.Error("Got nil result")
		}

		errval, ok := result.GetResponse().(*block_store.BlockStoreResponse_Error)
		if ok {
			log.Debugf("%v\n", addReq)
			t.Error("Got error adding block:", errval.Error.Message)
		}

		getNeReq := block_store.GetBlocksByHeightRequest{}
		getNeReq.HeadBlockId = nonExistentBlockID
		getNeReq.AncestorStartHeight = bt.ByNum[num].GetHeader().GetHeight() - 1
		if getNeReq.AncestorStartHeight == 0 {
			getNeReq.AncestorStartHeight = 1
		}
		getNeReq.NumBlocks = 1
		getNeReq.ReturnBlock = false
		getNeReq.ReturnReceipt = false

		iNeReq := block_store.BlockStoreRequest_GetBlocksByHeight{GetBlocksByHeight: &getNeReq}
		genericNeReq := block_store.BlockStoreRequest{Request: &iNeReq}

		result = handler.HandleRequest(&genericNeReq)
		errval, ok = result.GetResponse().(*block_store.BlockStoreResponse_Error)
		if !ok {
			t.Error("Expected error adding block")
		} else {
			blockNotPresent := BlockNotPresent{nonExistentBlockID}
			if string(errval.Error.Message) != blockNotPresent.Error() {
				t.Error("Unexpected error text")
			}
		}
	}
}

func addBlocksTestImpl(t *testing.T, backendType int, addZeroBlock bool) {
	b := NewBackend(backendType)
	handler := RequestHandler{Backend: b}

	// A compact notation of the tree of forks we want to create for the test
	//
	// The notation here expresses the following forks:
	//
	// 101 -> 102 -> 103 -> 104 -> 105 -> 106 -> 107 -> 108 -> 109 -> 110 -> 111 -> 112 -> 113 -> 114 -> 115 -> 116 -> 117 -> 118 -> 119 -> 120
	//                |                    |                    |                    |
	//                |                    |                    |---> 510 -> 511     |---> 613 -> 614
	//                |                    |
	//                |                    |---> 407 -> 408 -> 409 -> 410 -> 411 -> 412 -> 413 -> 414 -> 415 -> 416 -> 417 -> 418 -> 419
	//                |                                                       |
	//                |                                                       |---> 712 -> 713 -> 714 -> 715 -> 716 -> 717 -> 718
	//                |                                                                            |
	//                |                                                                            |---> 815 -> 816 -> 817 -> 818 -> 819
	//                |
	//                |---> 204 -> 205 -> 206 -> 207 -> 208 -> 209 -> 210 -> 211
	//                |
	//                |---> 304 -> 305 -> 306 -> 307
	//
	tree := [][]uint64{
		{0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120},
		{103, 204, 205, 206, 207, 208, 209, 210, 211},
		{103, 304, 305, 306, 307},
		{106, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419},
		{109, 510, 511},
		{112, 613, 614},
		{411, 712, 713, 714, 715, 716, 717, 718},
		{714, 815, 816, 817, 818, 819},
	}

	// A compact notation of the history of each of the heads
	//
	// Each line expresses a sequence from root to tip.
	// We use this table to test NumBlocks > 1 cases
	//
	treeHist := [][]uint64{
		{0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120},
		{0, 101, 102, 103, 204, 205, 206, 207, 208, 209, 210, 211},
		{0, 101, 102, 103, 304, 305, 306, 307},
		{0, 101, 102, 103, 104, 105, 106, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419},
		{0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 510, 511},
		{0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 613, 614},
		{0, 101, 102, 103, 104, 105, 106, 407, 408, 409, 410, 411, 712, 713, 714, 715, 716, 717, 718},
		{0, 101, 102, 103, 104, 105, 106, 407, 408, 409, 410, 411, 712, 713, 714, 815, 816, 817, 818, 819},
	}

	// A compact notation of the test cases for GetAncestorAtHeight
	//
	// Item {105, 120, 4, 104} means for blocks 105-120, the ancestor at height 4 is block 104.
	//
	ancestorCases := [][]uint64{
		{101, 120, 1, 101}, {102, 120, 2, 102}, {103, 120, 3, 103}, {104, 120, 4, 104},
		{105, 120, 5, 105}, {106, 120, 6, 106}, {107, 120, 7, 107}, {108, 120, 8, 108}, {109, 120, 9, 109},
		{110, 120, 10, 110}, {111, 120, 11, 111}, {112, 120, 12, 112}, {113, 120, 13, 113}, {114, 120, 14, 114},
		{115, 120, 15, 115}, {116, 120, 16, 116}, {117, 120, 17, 117}, {118, 120, 18, 118}, {119, 120, 19, 119},
		{120, 120, 20, 120},

		{204, 211, 1, 101}, {204, 211, 2, 102}, {204, 211, 3, 103}, {204, 211, 4, 204},
		{205, 211, 5, 205}, {206, 211, 6, 206}, {207, 211, 7, 207}, {208, 211, 8, 208}, {209, 211, 9, 209},
		{210, 211, 10, 210}, {211, 211, 11, 211},

		{304, 307, 1, 101}, {304, 307, 2, 102}, {304, 307, 3, 103}, {304, 307, 4, 304},
		{305, 307, 5, 305}, {306, 307, 6, 306}, {307, 307, 7, 307},

		{407, 419, 1, 101}, {407, 419, 2, 102}, {407, 419, 3, 103}, {407, 419, 4, 104},
		{407, 419, 5, 105}, {407, 419, 6, 106}, {407, 419, 7, 407}, {408, 419, 8, 408}, {409, 419, 9, 409},
		{410, 419, 10, 410}, {411, 419, 11, 411}, {412, 419, 12, 412}, {413, 419, 13, 413}, {414, 419, 14, 414},
		{415, 419, 15, 415}, {416, 419, 16, 416}, {417, 419, 17, 417}, {418, 419, 18, 418}, {419, 419, 19, 419},

		{510, 511, 1, 101}, {510, 511, 2, 102}, {510, 511, 3, 103}, {510, 511, 4, 104},
		{510, 511, 5, 105}, {510, 511, 6, 106}, {510, 511, 7, 107}, {510, 511, 8, 108}, {510, 511, 9, 109},
		{510, 511, 10, 510}, {511, 511, 11, 511},

		{613, 614, 1, 101}, {613, 614, 2, 102}, {613, 614, 3, 103}, {613, 614, 4, 104},
		{613, 614, 5, 105}, {613, 614, 6, 106}, {613, 614, 7, 107}, {613, 614, 8, 108}, {613, 614, 9, 109},
		{613, 614, 10, 110}, {613, 614, 11, 111}, {613, 614, 12, 112}, {613, 614, 13, 613}, {614, 614, 14, 614},

		{712, 718, 1, 101}, {712, 718, 2, 102}, {712, 718, 3, 103}, {712, 718, 4, 104},
		{712, 718, 5, 105}, {712, 718, 6, 106}, {712, 718, 7, 407}, {712, 718, 8, 408}, {712, 718, 9, 409},
		{712, 718, 10, 410}, {712, 718, 11, 411}, {712, 718, 12, 712}, {713, 718, 13, 713}, {714, 718, 14, 714},
		{715, 718, 15, 715}, {716, 718, 16, 716}, {717, 718, 17, 717}, {718, 718, 18, 718},

		{815, 819, 1, 101}, {815, 819, 2, 102}, {815, 819, 3, 103}, {815, 819, 4, 104},
		{815, 819, 5, 105}, {815, 819, 6, 106}, {815, 819, 7, 407}, {815, 819, 8, 408}, {815, 819, 9, 409},
		{815, 819, 10, 410}, {815, 819, 11, 411}, {815, 819, 12, 712}, {815, 819, 13, 713}, {815, 819, 14, 714},
		{815, 819, 15, 815}, {816, 819, 16, 816}, {817, 819, 17, 817}, {818, 819, 18, 818}, {819, 819, 19, 819},
	}

	if addZeroBlock {
		tree[0] = []uint64{0, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120}
	}

	mbt := NewMockBlockTree(tree)
	bt := ToBlockTree(mbt)

	for _, num := range bt.Numbers {
		bt.ByNum[num].Header.Height = uint64(num % 100)
	}

	BuildTestTree(t, &handler, bt)

	for i := 0; i < len(ancestorCases); i++ {
		for b := ancestorCases[i][0]; b <= ancestorCases[i][1]; b++ {
			blockID := bt.ByNum[b].Id
			height := ancestorCases[i][2]
			expectedAncestorID := bt.ByNum[ancestorCases[i][3]].Id

			getReq := block_store.GetBlocksByHeightRequest{}
			getReq.HeadBlockId = blockID
			getReq.AncestorStartHeight = height
			getReq.NumBlocks = 1
			getReq.ReturnBlock = false
			getReq.ReturnReceipt = false

			iReq := block_store.BlockStoreRequest_GetBlocksByHeight{GetBlocksByHeight: &getReq}
			genericReq := &block_store.BlockStoreRequest{Request: &iReq}

			_, err := json.Marshal(genericReq)
			if err != nil {
				t.Error("Could not marshal JSON", err)
			}

			result := handler.HandleRequest(genericReq)
			if result == nil {
				t.Error("Got nil result")
			}
			errval, ok := result.GetResponse().(*block_store.BlockStoreResponse_Error)
			if ok {
				t.Error("Got error retrieving block:", errval.Error.Message)
				t.FailNow()
			}

			resp := result.GetResponse().(*block_store.BlockStoreResponse_GetBlocksByHeight)
			if len(resp.GetBlocksByHeight.GetBlockItems()) != 1 {
				t.Error("Expected result of length 1")
			}

			if resp.GetBlocksByHeight.GetBlockItems()[0].BlockHeight != height {
				t.Errorf("Unexpected ancestor height:  Got %d, expected %d", resp.GetBlocksByHeight.GetBlockItems()[0].BlockHeight, height)
			}

			if !bytes.Equal(resp.GetBlocksByHeight.GetBlockItems()[0].GetBlockId(), expectedAncestorID) {
				t.Error("Unexpected ancestor block ID")
			}
		}
	}

	for _, num := range bt.Numbers {
		blockID := bt.ByNum[num].Id
		height := bt.ByNum[num].Header.Height

		getReq := block_store.GetBlocksByHeightRequest{}
		getReq.HeadBlockId = blockID
		getReq.NumBlocks = 1
		getReq.ReturnBlock = false
		getReq.ReturnReceipt = false

		// GetAncestorAtHeight where the requested height is equal to the height of the requested head
		getReq.AncestorStartHeight = height + 1

		iReq := block_store.BlockStoreRequest_GetBlocksByHeight{GetBlocksByHeight: &getReq}
		genericReq := &block_store.BlockStoreRequest{Request: &iReq}

		result := handler.HandleRequest(genericReq)
		if result == nil {
			t.Error("Got nil result")
		}
		errval, ok := result.GetResponse().(*block_store.BlockStoreResponse_Error)
		if !ok {
			t.Error("Unexpectedly got non-error result attempting to retrieve descendant block:", result)
		} else {
			if errval.Error.Message != "Block height mismatch" {
				t.Error("Unexpected error text")
			}
		}
	}

	// Check querying all possible past ranges from head at end of sequence
	for i := 0; i < len(tree); i++ {
		headIndex := len(tree[i]) - 1
		headNum := tree[i][headIndex]
		headID := bt.ByNum[headNum].Id

		for j := 1; j < len(treeHist[i]); j++ {
			// Iterate beyond the tree
			kMax := len(treeHist[i]) + 5

			for k := j; k < kMax; k++ {
				getReq := block_store.GetBlocksByHeightRequest{}
				getReq.HeadBlockId = headID
				getReq.NumBlocks = uint32(k - j)
				getReq.ReturnBlock = false
				getReq.ReturnReceipt = false
				getReq.AncestorStartHeight = uint64(j)

				iReq := block_store.BlockStoreRequest_GetBlocksByHeight{GetBlocksByHeight: &getReq}
				genericReq := block_store.BlockStoreRequest{Request: &iReq}

				result := handler.HandleRequest(&genericReq)
				if result == nil {
					t.Error("Got nil result")
				}
				errval, ok := result.GetResponse().(*block_store.BlockStoreResponse_Error)
				if ok {
					t.Error("GetBlocksByHeightReq returned error:", errval.Error.Message)
				}

				endIndex := k
				if endIndex > len(treeHist[i]) {
					endIndex = len(treeHist[i])
				}
				blockSeq := treeHist[i][j:endIndex]

				resp := result.GetResponse().(*block_store.BlockStoreResponse_GetBlocksByHeight)
				if len(resp.GetBlocksByHeight.GetBlockItems()) != len(blockSeq) {
					t.Errorf("Unexpected result length, expected %d, got %d, expect array is %v", len(resp.GetBlocksByHeight.GetBlockItems()), len(blockSeq), blockSeq)
				}

				for checkIndex := 0; checkIndex < len(resp.GetBlocksByHeight.GetBlockItems()); checkIndex++ {
					expectedHeight := blockSeq[checkIndex] % 100
					if resp.GetBlocksByHeight.GetBlockItems()[checkIndex].BlockHeight != expectedHeight {
						t.Error("Unexpected block height in response")
					}
					expectedBlockID := bt.ByNum[blockSeq[checkIndex]].GetId()
					if !bytes.Equal(resp.GetBlocksByHeight.GetBlockItems()[checkIndex].GetBlockId(), expectedBlockID) {
						t.Error("Unexpected ancestor block ID")
					}
				}
			}
		}
	}

	// Test bad RPC
	byHeightReq := &block_store.GetBlocksByHeightRequest{
		HeadBlockId:         bt.ByNum[819].Id,
		AncestorStartHeight: 0,
		NumBlocks:           1,
		ReturnBlock:         true,
		ReturnReceipt:       false,
	}
	_, err := handler.GetBlocksByHeight(byHeightReq)
	if err == nil {
		t.Errorf("Excepted error for AncestorStartHeight == 0")
	}

	CloseBackend(b)
}

func TestAddBlocks(t *testing.T) {
	for backendType := range backendTypes {
		addBlocksTestImpl(t, backendType, false)
		addBlocksTestImpl(t, backendType, true)
	}
}

func testGetBlocksByIDImpl(t *testing.T, returnBlock bool, returnReceipt bool) {
	tree := [][]uint64{
		{0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113},
		{103, 204, 205, 206, 207, 208, 209, 210, 211},
		{103, 304, 305, 306, 307},
	}

	b := NewMapBackend()
	handler := RequestHandler{Backend: b}
	mbt := NewMockBlockTree(tree)
	for _, mb := range mbt.ByNum {
		mb.Receipt = []byte(fmt.Sprintf("Receipt for block %d", mb.Num))
	}
	bt := ToBlockTree(mbt)
	BuildTestTree(t, &handler, bt)

	getID := func(num uint64) []byte {
		if num < 900 {
			return bt.ByNum[num].GetId()
		}
		return GetNonExistentBlockID(num)
	}

	getBlocksByID := func(ids []uint64, returnBlock bool, returnReceipt bool, errText string) []*block_store.BlockItem {
		req := block_store.GetBlocksByIdRequest{}
		req.BlockIds = make([][]byte, len(ids))
		for i := 0; i < len(ids); i++ {
			req.BlockIds[i] = getID(ids[i])
		}
		req.ReturnBlock = returnBlock
		req.ReturnReceipt = returnReceipt

		iReq := block_store.BlockStoreRequest_GetBlocksById{GetBlocksById: &req}
		genericReq := block_store.BlockStoreRequest{Request: &iReq}

		result := handler.HandleRequest(&genericReq)
		if result == nil {
			t.Error("Got nil result")
		}
		errval, isErr := result.GetResponse().(*block_store.BlockStoreResponse_Error)
		if errText == "" {
			if isErr {
				t.Error("GetBlocksByIDReq returned error (expecting success):", errval.Error.Message)
			}
			// OK:  Expected success, got success
		} else {
			if isErr {
				if errText != string(errval.Error.Message) {
					t.Error("GetBlocksByIDReq returned unexpected error:", errval.Error.Message)
				}
				// OK:  Expected error, got error, errText matched
				return []*block_store.BlockItem{}
			}
			t.Error("GetBlocksByIDReq returned success, expected error was:", errText)
		}
		return result.GetResponse().(*block_store.BlockStoreResponse_GetBlocksById).GetBlocksById.GetBlockItems()
	}

	testCases := [][]uint64{
		{}, {101, 102, 103}, {108, 109, 110}, {206, 104, 307, 111},
		{990}, {990, 991}, {990, 108, 991, 992, 104},
	}

	var checkBlockLength func(*block_store.BlockItem)
	var checkReceiptLength func(*block_store.BlockItem)

	if returnBlock {
		checkBlockLength = func(item *block_store.BlockItem) {
			if item.GetBlock() == nil {
				t.Error("Expected non-empty block")
			}
		}
	} else {
		checkBlockLength = func(item *block_store.BlockItem) {
			if item.GetBlock() != nil {
				t.Error("Expected empty block")
			}
		}
	}

	if returnReceipt {
		checkReceiptLength = func(item *block_store.BlockItem) {
			// TODO Fix this when internal representation of block receipt is fixed.
			// if !item.BlockReceipt.HasValue() {
			// 	t.Error("Expected non-empty receipt")
			// }
		}
	} else {
		checkReceiptLength = func(item *block_store.BlockItem) {
			if item.GetReceipt() != nil {
				t.Error("Expected empty receipt")
			}
		}
	}

	checkLengths := func(item *block_store.BlockItem) {
		checkBlockLength(item)
		checkReceiptLength(item)
	}

	for i := 0; i < len(testCases); i++ {
		result := getBlocksByID(testCases[i], returnBlock, returnReceipt, "")
		if len(result) != len(testCases[i]) {
			t.Error("Unexpected result length")
		}

		for j := 0; j < len(testCases[i]); j++ {
			if testCases[i][j] < 900 {
				expectedBlockID := getID(testCases[i][j])
				if !bytes.Equal(result[j].GetBlockId(), expectedBlockID) {
					fmt.Printf("%d %d %v %v\n", i, j, expectedBlockID, result[j].GetBlockId())
					t.Error("Unexpected block ID")
					return
				}
				if uint64(result[j].BlockHeight) != testCases[i][j]%100 {
					t.Error("Unexpected block height")
				}
				checkLengths(result[j])
			} else {
				expectedBlockID := []byte{}
				if !bytes.Equal(result[j].GetBlockId(), expectedBlockID) {
					t.Error("Expected empty multihash for non-existent block")
				}
				if result[j].BlockHeight != 0 {
					t.Error("Expected zero height for non-existent block")
				}
			}
		}
	}
}

func TestGetBlocksByID(t *testing.T) {
	for _, returnBlock := range []bool{false, true} {
		for _, returnReceipt := range []bool{false, true} {
			testGetBlocksByIDImpl(t, returnBlock, returnReceipt)
		}
	}
}

type TxnErrorBackend struct {
}

func (backend *TxnErrorBackend) Reset() error {
	return nil
}

// Put returns an error
func (backend *TxnErrorBackend) Put(key []byte, value []byte) error {
	return errors.New("Error on put")
}

// Get gets an error
func (backend *TxnErrorBackend) Get(key []byte) ([]byte, error) {
	return nil, errors.New("Error on get")
}

type TxnBadBackend struct {
}

func (backend *TxnBadBackend) Reset() error {
	return nil
}

// Put returns an error
func (backend *TxnBadBackend) Put(key []byte, value []byte) error {
	return nil
}

// Get gets an error
func (backend *TxnBadBackend) Get(key []byte) ([]byte, error) {
	return []byte{0, 0, 255, 255, 255, 255, 255}, nil
}

type TxnLongBackend struct {
}

func (backend *TxnLongBackend) Reset() error {
	return nil
}

// Put returns an error
func (backend *TxnLongBackend) Put(key []byte, value []byte) error {
	return nil
}

// Get gets an error
func (backend *TxnLongBackend) Get(key []byte) ([]byte, error) {
	return []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nil
}

func TestGetHighestBlock(t *testing.T) {
	for bType := range backendTypes {
		blockID, _ := multihash.EncodeName([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A}, "sha2-256")
		previousID, _ := multihash.EncodeName([]byte{0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14}, "sha2-256")
		height := uint64(2)

		topology := koinos.BlockTopology{Id: blockID, Previous: previousID, Height: height}

		b := NewBackend(bType)
		handler := RequestHandler{Backend: b}

		iReq := block_store.GetHighestBlockRequest{}
		ghbReq := block_store.BlockStoreRequest_GetHighestBlock{GetHighestBlock: &iReq}
		blockStoreReq := block_store.BlockStoreRequest{Request: &ghbReq}
		result := handler.HandleRequest(&blockStoreReq)

		errorResponse, ok := result.GetResponse().(*block_store.BlockStoreResponse_Error)
		if !ok {
			t.Error("Did not recieve expected response")
		}

		unexpectedHeightErr := UnexpectedHeightError{}
		if string(errorResponse.Error.Message) != unexpectedHeightErr.Error() {
			t.Error("Unexpected error")
		}

		if err := handler.UpdateHighestBlock(&topology); err != nil {
			t.Error(err)
		}

		iReq = block_store.GetHighestBlockRequest{}
		ghbReq = block_store.BlockStoreRequest_GetHighestBlock{GetHighestBlock: &iReq}
		blockStoreReq = block_store.BlockStoreRequest{Request: &ghbReq}
		result = handler.HandleRequest(&blockStoreReq)

		highestBlockResponse, ok := result.GetResponse().(*block_store.BlockStoreResponse_GetHighestBlock)
		if !ok {
			t.Error("Did not recieve expected response")
		}

		if !bytes.Equal(highestBlockResponse.GetHighestBlock.GetTopology().GetId(), blockID) {
			t.Error("Encountered an ID mismatch")
		}

		if !bytes.Equal(highestBlockResponse.GetHighestBlock.GetTopology().GetPrevious(), previousID) {
			t.Error("Encountered an ID mismatch")
		}

		if highestBlockResponse.GetHighestBlock.GetTopology().GetHeight() != height {
			t.Error("Encountered a height mismatch")
		}

		// Create a lower block, ensure highest block is still the former

		lowerBlockID, _ := multihash.EncodeName([]byte{0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E}, "sha2-256")
		lowerPreviousID, _ := multihash.EncodeName([]byte{0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28}, "sha2-256")
		lowerHeight := uint64(1)

		lowerTopology := koinos.BlockTopology{Id: lowerBlockID, Previous: lowerPreviousID, Height: lowerHeight}
		if err := handler.UpdateHighestBlock(&lowerTopology); err != nil {
			t.Error(err)
		}

		iReq = block_store.GetHighestBlockRequest{}
		ghbReq = block_store.BlockStoreRequest_GetHighestBlock{GetHighestBlock: &iReq}
		blockStoreReq = block_store.BlockStoreRequest{Request: &ghbReq}
		result = handler.HandleRequest(&blockStoreReq)

		highestBlockResponse, ok = result.GetResponse().(*block_store.BlockStoreResponse_GetHighestBlock)
		if !ok {
			t.Error("Did not recieve expected response")
		}

		if !bytes.Equal(highestBlockResponse.GetHighestBlock.GetTopology().GetId(), blockID) {
			t.Error("Encountered an ID mismatch")
		}

		if !bytes.Equal(highestBlockResponse.GetHighestBlock.GetTopology().GetPrevious(), previousID) {
			t.Error("Encountered an ID mismatch")
		}

		if highestBlockResponse.GetHighestBlock.GetTopology().GetHeight() != height {
			t.Error("Encountered a height mismatch")
		}

		// Create a new highest block, ensure it is now the highest block in the block store

		higherBlockID, _ := multihash.EncodeName([]byte{0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E}, "sha2-256")
		higherPreviousID, _ := multihash.EncodeName([]byte{0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28}, "sha2-256")
		higherHeight := uint64(3)

		higherTopology := koinos.BlockTopology{Id: higherBlockID, Previous: higherPreviousID, Height: higherHeight}
		if err := handler.UpdateHighestBlock(&higherTopology); err != nil {
			t.Error(err)
		}

		iReq = block_store.GetHighestBlockRequest{}
		ghbReq = block_store.BlockStoreRequest_GetHighestBlock{GetHighestBlock: &iReq}
		blockStoreReq = block_store.BlockStoreRequest{Request: &ghbReq}
		result = handler.HandleRequest(&blockStoreReq)

		highestBlockResponse, ok = result.GetResponse().(*block_store.BlockStoreResponse_GetHighestBlock)
		if !ok {
			t.Error("Did not recieve expected response")
		}

		if !bytes.Equal(highestBlockResponse.GetHighestBlock.GetTopology().GetId(), higherBlockID) {
			t.Error("Encountered an ID mismatch")
		}

		if !bytes.Equal(highestBlockResponse.GetHighestBlock.GetTopology().GetPrevious(), higherPreviousID) {
			t.Error("Encountered an ID mismatch")
		}

		if highestBlockResponse.GetHighestBlock.GetTopology().GetHeight() != higherHeight {
			t.Error("Encountered a height mismatch")
		}
	}
}

func TestInternalError(t *testing.T) {
	err := InternalError{}
	if err.Error() != "Internal constraint was violated" {
		t.Error("Unexpected error text")
	}
}
