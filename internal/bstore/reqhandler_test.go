package bstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dgraph-io/badger"

	types "github.com/koinos/koinos-types-golang"
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
		break
	case BadgerBackendType:
		dirname, err := ioutil.TempDir(os.TempDir(), "bstore-test-*")
		if err != nil {
			panic("unable to create temp directory")
		}
		opts := badger.DefaultOptions(dirname)
		backend = NewBadgerBackend(opts)
		break
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
		break
	default:
		panic("unknown backend type")
	}
}

func TestHandleReservedRequest(t *testing.T) {
	for bType := range backendTypes {
		b := NewBackend(bType)
		handler := RequestHandler{b}

		testReq := types.BlockStoreRequest{Value: types.NewBlockStoreReservedRequest()}
		result := handler.HandleRequest(&testReq)

		errval, ok := result.Value.(*types.BlockStoreErrorResponse)
		if !ok {
			t.Error("Should have errored BlockStoreError")
		}
		if errval.ErrorText != "Reserved request is not supported" {
			t.Error("Unexpected error text")
		}
		CloseBackend(b)
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

func GetBlockID(num int64) types.Multihash {
	if num < 0 {
		return GetEmptyBlockID()
	}

	dataBytes := make([]byte, binary.MaxVarintLen64)
	count := binary.PutUvarint(dataBytes, uint64(num))

	hash := sha256.Sum256(dataBytes[:count])

	var vb types.VariableBlob = types.VariableBlob(hash[:])

	return types.Multihash{ID: 0x12, Digest: vb}
}

func GetEmptyBlockID() types.Multihash {
	vb := types.VariableBlob(make([]byte, 32))
	return types.Multihash{ID: 0x12, Digest: vb}
}

func GetBlockBody(num int64, prev int64) *types.Block {
	return &types.Block{
		ID: GetBlockID(num),
		Header: types.BlockHeader{
			Previous: GetBlockID(prev),
			Height:   types.BlockHeightType(num),
		},
		ActiveData:  *types.NewOpaqueActiveBlockData(),
		PassiveData: *types.NewOpaquePassiveBlockData(),
	}
}

func GetBlockReceipt(num int64) *types.VariableBlob {
	vb := types.VariableBlob([]byte(fmt.Sprintf("Receipt for block %d", num)))
	return &vb
}

func BuildTestTree(t *testing.T, handler *RequestHandler, tree [][]int64, addZeroBlock bool) {
	if addZeroBlock {
		addReq := types.AddBlockRequest{}
		addReq.BlockToAdd.Block = *types.NewOpaqueBlockFromNative(*GetBlockBody(0, -1))
		addReq.BlockToAdd.BlockReceipt = *types.NewOpaqueBlockReceiptFromBlob(GetBlockReceipt(0))

		genericReq := types.BlockStoreRequest{Value: &addReq}

		result := handler.HandleRequest(&genericReq)
		errval, ok := result.Value.(*types.BlockStoreErrorResponse)
		if ok {
			t.Error("Could not add block 0: ", errval.ErrorText)
		}
	}

	nonExistentBlockID := GetBlockID(999)

	for i := 0; i < len(tree); i++ {
		for j := 1; j < len(tree[i]); j++ {
			addReq := types.AddBlockRequest{}
			addReq.BlockToAdd.Block = *types.NewOpaqueBlockFromNative(types.Block{
				ID: GetBlockID(tree[i][j]),
				Header: types.BlockHeader{
					Previous: GetBlockID(tree[i][j-1]),
					Height:   types.BlockHeightType(tree[i][j] % 100),
				},
				ActiveData:  *types.NewOpaqueActiveBlockData(),
				PassiveData: *types.NewOpaquePassiveBlockData(),
			})

			addReq.BlockToAdd.BlockReceipt = *types.NewOpaqueBlockReceiptFromBlob(GetBlockReceipt(tree[i][j]))

			genericReq := types.BlockStoreRequest{Value: &addReq}

			_, err := json.Marshal(genericReq)
			if err != nil {
				t.Error("Could not marshal JSON", err)
			}

			result := handler.HandleRequest(&genericReq)
			if result == nil {
				t.Error("Got nil result")
			}
			errval, ok := result.Value.(*types.BlockStoreErrorResponse)
			if ok {
				fmt.Printf("%v\n", addReq)
				t.Error("Got error adding block:", errval.ErrorText)
			}

			getNeReq := types.GetBlocksByHeightRequest{}
			getNeReq.HeadBlockID = nonExistentBlockID
			getNeReq.AncestorStartHeight = types.BlockHeightType(j - 1)
			getNeReq.NumBlocks = 1
			getNeReq.ReturnBlock = false
			getNeReq.ReturnReceipt = false

			genericNeReq := types.BlockStoreRequest{Value: &getNeReq}
			_, err = json.Marshal(genericNeReq)
			if err != nil {
				t.Error("Could not marshal JSON", err)
			}

			result = handler.HandleRequest(&genericNeReq)
			errval, ok = result.Value.(*types.BlockStoreErrorResponse)
			if !ok {
				t.Error("Expected error adding block")
			} else {
				blockNotPresent := BlockNotPresent{nonExistentBlockID}
				if string(errval.ErrorText) != blockNotPresent.Error() {
					t.Error("Unexpected error text")
				}
			}
		}
	}
}

func addBlocksTestImpl(t *testing.T, backendType int, addZeroBlock bool) {
	b := NewBackend(backendType)
	handler := RequestHandler{b}

	// A compact notation of the tree of forks we want to create for the test
	tree := [][]int64{
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
	treeHist := [][]int64{
		{0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120},
		{0, 101, 102, 103, 204, 205, 206, 207, 208, 209, 210, 211},
		{0, 101, 102, 103, 304, 305, 306, 307},
		{0, 101, 102, 103, 104, 105, 106, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419},
		{0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 510, 511},
		{0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 613, 614},
		{0, 101, 102, 103, 104, 105, 106, 407, 408, 409, 410, 411, 712, 713, 714, 715, 716, 717, 718},
		{0, 101, 102, 103, 104, 105, 106, 407, 408, 409, 410, 411, 712, 713, 714, 815, 816, 817, 818, 819},
	}

	// Item {105, 120, 4, 104} means for blocks 105-120, the ancestor at height 4 is block 104.
	ancestorCases := [][]int64{
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

	BuildTestTree(t, &handler, tree, addZeroBlock)

	for i := 0; i < len(ancestorCases); i++ {
		for b := ancestorCases[i][0]; b <= ancestorCases[i][1]; b++ {
			blockID := GetBlockID(b)
			height := ancestorCases[i][2]
			expectedAncestorID := GetBlockID(ancestorCases[i][3])

			getReq := types.GetBlocksByHeightRequest{}
			getReq.HeadBlockID = blockID
			getReq.AncestorStartHeight = types.BlockHeightType(height)
			getReq.NumBlocks = 1
			getReq.ReturnBlock = false
			getReq.ReturnReceipt = false

			genericReq := types.BlockStoreRequest{Value: &getReq}

			_, err := json.Marshal(genericReq)
			if err != nil {
				t.Error("Could not marshal JSON", err)
			}

			result := handler.HandleRequest(&genericReq)
			if result == nil {
				t.Error("Got nil result")
			}
			errval, ok := result.Value.(*types.BlockStoreErrorResponse)
			if ok {
				t.Error("Got error retrieving block:", errval.ErrorText)
				t.FailNow()
			}

			resp := result.Value.(*types.GetBlocksByHeightResponse)
			if len(resp.BlockItems) != 1 {
				t.Error("Expected result of length 1")
			}

			if resp.BlockItems[0].BlockHeight != types.BlockHeightType(height) {
				t.Errorf("Unexpected ancestor height:  Got %d, expected %d", resp.BlockItems[0].BlockHeight, height)
			}

			if !resp.BlockItems[0].BlockID.Equals(&expectedAncestorID) {
				t.Error("Unexpected ancestor block ID")
			}
		}
	}

	for i := 0; i < len(tree); i++ {
		for j := 1; j < len(tree[i]); j++ {
			blockID := GetBlockID(tree[i][j])
			height := tree[i][j] % 100

			getReq := types.GetBlocksByHeightRequest{}
			getReq.HeadBlockID = blockID
			getReq.NumBlocks = 1
			getReq.ReturnBlock = false
			getReq.ReturnReceipt = false

			// GetAncestorAtHeight where the requested height is equal to the height of the requested head
			getReq.AncestorStartHeight = types.BlockHeightType(height + 1)

			genericReq := types.BlockStoreRequest{Value: &getReq}

			result := handler.HandleRequest(&genericReq)
			if result == nil {
				t.Error("Got nil result")
			}
			errval, ok := result.Value.(*types.BlockStoreErrorResponse)
			if !ok {
				t.Error("Unexpectedly got non-error result attempting to retrieve descendant block:", result)
			} else {
				if errval.ErrorText != "Block height mismatch" {
					t.Error("Unexpected error text")
				}
			}
		}
	}

	// Check querying all possible past ranges from head at end of sequence
	for i := 0; i < len(tree); i++ {
		headIndex := len(tree[i]) - 1
		headID := GetBlockID(tree[i][headIndex])

		for j := 1; j < len(treeHist[i]); j++ {
			// Iterate beyond the tree
			kMax := len(treeHist[i]) + 5

			for k := j; k < kMax; k++ {
				getReq := types.GetBlocksByHeightRequest{}
				getReq.HeadBlockID = headID
				getReq.NumBlocks = types.UInt32(k - j)
				getReq.ReturnBlock = false
				getReq.ReturnReceipt = false
				getReq.AncestorStartHeight = types.BlockHeightType(j)

				genericReq := types.BlockStoreRequest{Value: &getReq}

				result := handler.HandleRequest(&genericReq)
				if result == nil {
					t.Error("Got nil result")
				}
				errval, ok := result.Value.(*types.BlockStoreErrorResponse)
				if ok {
					t.Error("GetBlocksByHeightReq returned error:", errval.ErrorText)
				}

				endIndex := k
				if endIndex > len(treeHist[i]) {
					endIndex = len(treeHist[i])
				}
				blockSeq := treeHist[i][j:endIndex]

				resp := result.Value.(*types.GetBlocksByHeightResponse)
				if len(resp.BlockItems) != len(blockSeq) {
					t.Errorf("Unexpected result length, expected %d, got %d, expect array is %v", len(resp.BlockItems), len(blockSeq), blockSeq)
				}

				for checkIndex := 0; checkIndex < len(resp.BlockItems); checkIndex++ {
					expectedHeight := types.BlockHeightType(blockSeq[checkIndex] % 100)
					if resp.BlockItems[checkIndex].BlockHeight != expectedHeight {
						t.Error("Unexpected block height in response")
					}
					expectedBlockID := GetBlockID(blockSeq[checkIndex])
					if !resp.BlockItems[checkIndex].BlockID.Equals(&expectedBlockID) {
						t.Error("Unexpected ancestor block ID")
					}
				}
			}
		}
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
	tree := [][]int64{
		{0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113},
		{0, 101, 102, 103, 204, 205, 206, 207, 208, 209, 210, 211},
		{0, 101, 102, 103, 304, 305, 306, 307},
	}

	b := NewMapBackend()
	handler := RequestHandler{b}
	BuildTestTree(t, &handler, tree, true)

	getBlocksByID := func(ids []int64, returnBlock bool, returnReceipt bool, errText string) []types.BlockItem {
		req := types.NewGetBlocksByIDRequest()
		req.BlockID = make([]types.Multihash, len(ids))
		for i := 0; i < len(ids); i++ {
			req.BlockID[i] = GetBlockID(ids[i])
		}
		req.ReturnBlockBlob = types.Boolean(returnBlock)
		req.ReturnReceiptBlob = types.Boolean(returnReceipt)

		genericReq := types.BlockStoreRequest{Value: req}

		result := handler.HandleRequest(&genericReq)
		if result == nil {
			t.Error("Got nil result")
		}
		errval, isErr := result.Value.(*types.BlockStoreErrorResponse)
		if errText == "" {
			if isErr {
				t.Error("GetBlocksByIDReq returned error (expecting success):", errval.ErrorText)
			}
			// OK:  Expected success, got success
		} else {
			if isErr {
				if errText != string(errval.ErrorText) {
					t.Error("GetBlocksByIDReq returned unexpected error:", errval.ErrorText)
				}
				// OK:  Expected error, got error, errText matched
				return []types.BlockItem{}
			}
			t.Error("GetBlocksByIDReq returned success, expected error was:", errText)
		}
		return result.Value.(*types.GetBlocksByIDResponse).BlockItems
	}

	testCases := [][]int64{
		{}, {101, 102, 103}, {108, 109, 110}, {206, 104, 307, 111},
		{990}, {990, 991}, {990, 108, 991, 992, 104},
	}

	var checkBlockLength func(*types.BlockItem)
	var checkReceiptLength func(*types.BlockItem)

	if returnBlock {
		checkBlockLength = func(item *types.BlockItem) {
			if len(*item.Block.GetBlob()) == 0 {
				t.Error("Expected non-empty block")
			}
		}
	} else {
		checkBlockLength = func(item *types.BlockItem) {
			if len(*item.Block.GetBlob()) > 0 {
				t.Error("Expected empty block")
			}
		}
	}

	if returnReceipt {
		checkReceiptLength = func(item *types.BlockItem) {
			if len(*item.BlockReceipt.GetBlob()) == 0 {
				t.Error("Expected non-empty receipt")
			}
		}
	} else {
		checkReceiptLength = func(item *types.BlockItem) {
			if len(*item.BlockReceipt.GetBlob()) > 0 {
				t.Error("Expected empty receipt")
			}
		}
	}

	checkLengths := func(item *types.BlockItem) {
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
				expectedBlockID := GetBlockID(testCases[i][j])
				if !result[j].BlockID.Equals(&expectedBlockID) {
					fmt.Printf("%d %d %v %v\n", i, j, expectedBlockID, result[j].BlockID)
					t.Error("Unexpected block ID")
					return
				}
				if int64(result[j].BlockHeight) != testCases[i][j]%100 {
					t.Error("Unexpected block height")
				}
				checkLengths(&result[j])
			} else {
				expectedBlockID := types.NewMultihash()
				if !result[j].BlockID.Equals(expectedBlockID) {
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

func GetAddTransactionReq(n types.UInt64) types.AddTransactionRequest {
	vb := n.Serialize(types.NewVariableBlob())
	m := types.Multihash{ID: 0x12, Digest: *vb}
	r := types.AddTransactionRequest{
		Transaction: types.Transaction{
			ID:          m,
			ActiveData:  *types.NewOpaqueActiveTransactionData(),
			PassiveData: *types.NewOpaquePassiveTransactionData(),
		},
	}
	return r
}

func GetGetTransactionsByIDReq(start uint64, num uint64) types.GetTransactionsByIDRequest {
	vm := make([]types.Multihash, 0)
	for i := types.UInt64(start); i < types.UInt64(start+num); i++ {
		vb := i.Serialize(types.NewVariableBlob())
		m := types.Multihash{ID: 0x12, Digest: *vb}
		vm = append(vm, m)
	}

	r := types.GetTransactionsByIDRequest{TransactionIds: vm}
	return r
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

func TestAddTransaction(t *testing.T) {
	reqs := make([]types.AddTransactionRequest, 32)
	for i := 0; i < 32; i++ {
		reqs[i] = GetAddTransactionReq(types.UInt64(i))
	}

	// Add the transactions
	for bType := range backendTypes {
		b := NewBackend(bType)
		handler := RequestHandler{b}
		for _, req := range reqs {
			bsr := types.BlockStoreRequest{Value: &req}
			result := handler.HandleRequest(&bsr)
			if result == nil {
				t.Error("Got nil result")
			}
			errval, ok := result.Value.(*types.BlockStoreErrorResponse)
			if ok {
				t.Error("Got error adding transaction:", errval.ErrorText)
			}
		}

		// Test adding an already existing transaction
		{
			bsr := types.BlockStoreRequest{Value: &reqs[0]}
			result := handler.HandleRequest(&bsr)
			if result == nil {
				t.Error("Got nil result")
			}
			errval, ok := result.Value.(*types.BlockStoreErrorResponse)
			if ok {
				t.Error("Got error adding transaction:", errval.ErrorText)
			}
		}

		// Fetch the transactions
		{
			r := GetGetTransactionsByIDReq(0, 32)
			bsr := types.BlockStoreRequest{Value: &r}
			result := handler.HandleRequest(&bsr)
			if result == nil {
				t.Error("Got nil result")
			}
			errval, ok := result.Value.(*types.BlockStoreErrorResponse)
			if ok {
				t.Error("Error fetching transactions:", errval.ErrorText)
			}

			tres, ok := result.Value.(*types.GetTransactionsByIDResponse)
			if !ok {
				t.Error("Result is wrong type")
			}

			for i, nt := range tres.TransactionItems {
				if !bytes.Equal(*&reqs[i].Transaction.ID.Digest, *&nt.Transaction.ID.Digest) {
					t.Error("Result does not match added transaction")
				}
			}
		}

		// Test fetching an invalid transaction
		{
			r := GetGetTransactionsByIDReq(64, 1)
			bsr := types.BlockStoreRequest{Value: &r}
			result := handler.HandleRequest(&bsr)
			if result == nil {
				t.Error("Got nil result")
			}
			errval, ok := result.Value.(*types.BlockStoreErrorResponse)
			if !ok {
				t.Error("Did not recieve expected error")
			} else if errval.ErrorText != "Transaction was not present" {
				t.Error("Did not recieve expected error text")
			}
		}

		CloseBackend(b)
	}

	// Test error on add
	{
		handler := RequestHandler{&TxnErrorBackend{}}
		r := GetAddTransactionReq(2)
		tr := types.BlockStoreRequest{Value: &r}
		result := handler.HandleRequest(&tr)
		errval, ok := result.Value.(*types.BlockStoreErrorResponse)
		if !ok {
			t.Error("Did not recieve expected error")
		} else if errval.ErrorText != "Error on put" {
			t.Error("Got unexpected error text: ", errval.ErrorText)
		}
	}

	// Test error on get
	{
		handler := RequestHandler{&TxnErrorBackend{}}
		r := GetGetTransactionsByIDReq(0, 1)
		tr := types.BlockStoreRequest{Value: &r}
		result := handler.HandleRequest(&tr)
		errval, ok := result.Value.(*types.BlockStoreErrorResponse)
		if !ok {
			t.Error("Did not recieve expected error")
		} else if errval.ErrorText != "Error on get" {
			t.Error("Got unexpected error text: ", errval.ErrorText)
		}
	}

	// Test bad record
	{
		handler := RequestHandler{&TxnBadBackend{}}
		r := GetGetTransactionsByIDReq(0, 1)
		tr := types.BlockStoreRequest{Value: &r}
		result := handler.HandleRequest(&tr)
		errval, ok := result.Value.(*types.BlockStoreErrorResponse)
		if !ok {
			t.Error("Did not recieve expected error")
		} else if errval.ErrorText != "Could not deserialize variable blob size" {
			t.Error("Got unexpected error text: ", errval.ErrorText)
		}
	}

	// Test too long record
	{
		handler := RequestHandler{&TxnLongBackend{}}
		r := GetGetTransactionsByIDReq(0, 1)
		tr := types.BlockStoreRequest{Value: &r}
		result := handler.HandleRequest(&tr)
		errval, ok := result.Value.(*types.BlockStoreErrorResponse)
		if !ok {
			t.Error("Did not recieve expected error")
		} else if errval.ErrorText != "Could not deserialize block" {
			t.Error("Got unexpected error text: ", errval.ErrorText)
		}
	}
}

func TestGetHighestBlock(t *testing.T) {
	for bType := range backendTypes {
		var blockID types.Multihash
		blockID.ID = 18
		blockID.Digest = types.VariableBlob{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A}

		var previousID types.Multihash
		previousID.ID = 18
		blockID.Digest = types.VariableBlob{0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14}

		var height types.BlockHeightType
		height = 2

		var topology types.BlockTopology
		topology.ID = blockID
		topology.Previous = previousID
		topology.Height = height

		b := NewBackend(bType)
		handler := RequestHandler{b}

		req := types.NewGetHighestBlockRequest()
		blockStoreReq := types.BlockStoreRequest{Value: req}
		result := handler.HandleRequest(&blockStoreReq)

		errorResponse, ok := result.Value.(*types.BlockStoreErrorResponse)
		if !ok {
			t.Error("Did not recieve expected response")
		}

		unexpectedHeightErr := UnexpectedHeightError{}
		if string(errorResponse.ErrorText) != unexpectedHeightErr.Error() {
			t.Error("Unexpected error")
		}

		handler.UpdateHighestBlock(&topology)

		req = types.NewGetHighestBlockRequest()
		blockStoreReq = types.BlockStoreRequest{Value: req}
		result = handler.HandleRequest(&blockStoreReq)

		highestBlockResponse, ok := result.Value.(*types.GetHighestBlockResponse)
		if !ok {
			t.Error("Did not recieve expected response")
		}

		if highestBlockResponse.Topology.ID.ID != blockID.ID {
			t.Error("Encountered an ID mismatch")
		}

		if !bytes.Equal(highestBlockResponse.Topology.ID.Digest, blockID.Digest) {
			t.Error("Encountered a digest mismatch")
		}

		if highestBlockResponse.Topology.Previous.ID != previousID.ID {
			t.Error("Encountered an ID mismatch")
		}

		if !bytes.Equal(highestBlockResponse.Topology.Previous.Digest, previousID.Digest) {
			t.Error("Encountered a digest mismatch")
		}

		if highestBlockResponse.Topology.Height != height {
			t.Error("Encountered a height mismatch")
		}

		var lowerBlockID types.Multihash
		lowerBlockID.ID = 18
		lowerBlockID.Digest = types.VariableBlob{0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E}

		var lowerPreviousID types.Multihash
		lowerPreviousID.ID = 18
		lowerPreviousID.Digest = types.VariableBlob{0x1F, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28}

		var lowerHeight types.BlockHeightType
		lowerHeight = 1

		var lowerTopology types.BlockTopology
		lowerTopology.ID = lowerBlockID
		lowerTopology.Previous = lowerPreviousID
		lowerTopology.Height = lowerHeight

		handler.UpdateHighestBlock(&lowerTopology)

		req = types.NewGetHighestBlockRequest()
		blockStoreReq = types.BlockStoreRequest{Value: req}
		result = handler.HandleRequest(&blockStoreReq)

		highestBlockResponse, ok = result.Value.(*types.GetHighestBlockResponse)
		if !ok {
			t.Error("Did not recieve expected response")
		}

		if highestBlockResponse.Topology.ID.ID != blockID.ID {
			t.Error("Encountered an ID mismatch")
		}

		if !bytes.Equal(highestBlockResponse.Topology.ID.Digest, blockID.Digest) {
			t.Error("Encountered a digest mismatch")
		}

		if highestBlockResponse.Topology.Previous.ID != previousID.ID {
			t.Error("Encountered an ID mismatch")
		}

		if !bytes.Equal(highestBlockResponse.Topology.Previous.Digest, previousID.Digest) {
			t.Error("Encountered a digest mismatch")
		}

		if highestBlockResponse.Topology.Height != height {
			t.Error("Encountered a height mismatch")
		}

		var higherBlockID types.Multihash
		higherBlockID.ID = 18
		higherBlockID.Digest = types.VariableBlob{0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, 0x31, 0x32}

		var higherPreviousID types.Multihash
		higherPreviousID.ID = 18
		higherPreviousID.Digest = types.VariableBlob{0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3A, 0x3B, 0x3C}

		var higherHeight types.BlockHeightType
		higherHeight = 3

		var higherTopology types.BlockTopology
		higherTopology.ID = higherBlockID
		higherTopology.Previous = higherPreviousID
		higherTopology.Height = higherHeight

		handler.UpdateHighestBlock(&higherTopology)

		req = types.NewGetHighestBlockRequest()
		blockStoreReq = types.BlockStoreRequest{Value: req}
		result = handler.HandleRequest(&blockStoreReq)

		highestBlockResponse, ok = result.Value.(*types.GetHighestBlockResponse)
		if !ok {
			t.Error("Did not recieve expected response")
		}

		if highestBlockResponse.Topology.ID.ID != higherBlockID.ID {
			t.Error("Encountered an ID mismatch")
		}

		if !bytes.Equal(highestBlockResponse.Topology.ID.Digest, higherBlockID.Digest) {
			t.Error("Encountered a digest mismatch")
		}

		if highestBlockResponse.Topology.Previous.ID != higherPreviousID.ID {
			t.Error("Encountered an ID mismatch")
		}

		if !bytes.Equal(highestBlockResponse.Topology.Previous.Digest, higherPreviousID.Digest) {
			t.Error("Encountered a digest mismatch")
		}

		if highestBlockResponse.Topology.Height != higherHeight {
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
