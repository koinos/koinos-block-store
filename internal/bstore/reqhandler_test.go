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

		testReq := types.BlockStoreReq{Value: types.NewReservedReq()}
		result, err := handler.HandleRequest(&testReq)
		if result != nil {
			t.Error("Result should be nil")
		}
		if err == nil {
			t.Error("Err should not be nil")
		}

		if _, ok := err.(*ReservedReqError); !ok {
			t.Error("Err should be ReservedReqError")
		}

		if err.Error() != "Reserved request is not supported" {
			t.Error("Unexpected error text")
		}
		CloseBackend(b)
	}
}

type UnknownReq struct {
}

func TestHandleUnknownRequestType(t *testing.T) {
	for bType := range backendTypes {
		b := NewBackend(bType)
		handler := RequestHandler{b}

		testReq := types.BlockStoreReq{Value: UnknownReq{}}
		result, err := handler.HandleRequest(&testReq)
		if result != nil {
			t.Error("Result should be nil")
		}
		if err == nil {
			t.Error("Err should not be nil")
		}

		if _, ok := err.(*UnknownReqError); !ok {
			t.Error("Err should be UnknownReqError")
		}
		if err.Error() != "Unknown request type" {
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

func GetBlockID(num uint64) types.Multihash {
	dataBytes := make([]byte, binary.MaxVarintLen64)
	count := binary.PutUvarint(dataBytes, num)

	hash := sha256.Sum256(dataBytes[:count])

	var vb types.VariableBlob = types.VariableBlob(hash[:])

	return types.Multihash{ID: 0x12, Digest: vb}
}

func GetEmptyBlockID() types.Multihash {
	vb := types.VariableBlob(make([]byte, 32))
	return types.Multihash{ID: 0x12, Digest: vb}
}

func GetBlockBody(num uint64) types.VariableBlob {
	greetings := []string{
		"Hello this is block %d.",
		"Greetings from block %d.",
		"I like being in block %d.",
	}

	return []byte(fmt.Sprintf(greetings[int(num)%len(greetings)], num))
}

func addBlocksTestImpl(t *testing.T, backendType int, addZeroBlock bool) {
	b := NewBackend(backendType)
	handler := RequestHandler{b}

	if addZeroBlock {
		addReq := types.AddBlockReq{}
		addReq.BlockToAdd.BlockID = GetBlockID(0)
		addReq.PreviousBlockID = GetEmptyBlockID()
		addReq.BlockToAdd.BlockHeight = 0
		addReq.BlockToAdd.BlockBlob = GetBlockBody(0)
		addReq.BlockToAdd.BlockReceiptBlob = types.VariableBlob(make([]byte, 0))

		genericReq := types.BlockStoreReq{Value: &addReq}

		_, err := handler.HandleRequest(&genericReq)
		if err != nil {
			t.Error("Could not add block 0", err)
		}
	}

	// A compact notation of the tree of forks we want to create for the test
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

	nonExistentBlockID := GetBlockID(999)

	for i := 0; i < len(tree); i++ {
		for j := 1; j < len(tree[i]); j++ {
			blockID := GetBlockID(tree[i][j])
			parentID := GetBlockID(tree[i][j-1])

			addReq := types.AddBlockReq{}
			addReq.BlockToAdd.BlockID = blockID
			addReq.PreviousBlockID = parentID
			addReq.BlockToAdd.BlockHeight = types.BlockHeightType(tree[i][j] % 100)
			addReq.BlockToAdd.BlockBlob = GetBlockBody(tree[i][j])
			addReq.BlockToAdd.BlockReceiptBlob = types.VariableBlob(make([]byte, 0))

			genericReq := types.BlockStoreReq{Value: &addReq}

			_, err := json.Marshal(genericReq)
			if err != nil {
				t.Error("Could not marshal JSON", err)
			}

			result, err := handler.HandleRequest(&genericReq)
			if err != nil {
				t.Error("Got error adding block:", err)
			}
			if result == nil {
				t.Error("Got nil result")
			}

			getNeReq := types.GetBlocksByHeightReq{}
			getNeReq.HeadBlockID = nonExistentBlockID
			getNeReq.AncestorStartHeight = types.BlockHeightType(j - 1)
			getNeReq.NumBlocks = 1
			getNeReq.ReturnBlockBlob = false
			getNeReq.ReturnReceiptBlob = false

			genericNeReq := types.BlockStoreReq{Value: &getNeReq}
			_, err = json.Marshal(genericNeReq)
			if err != nil {
				t.Error("Could not marshal JSON", err)
			}

			result, err = handler.HandleRequest(&genericNeReq)
			if (result != nil) || (err == nil) {
				t.Error("Expected error adding block")
			} else {
				if _, ok := err.(*BlockNotPresent); !ok {
					t.Error("Err should be BlockNotPresent")
				}
				if err.Error() != "Block was not present" {
					t.Error("Unexpected error text")
				}
			}
		}
	}

	// Item {105, 120, 4, 104} means for blocks 105-120, the ancestor at height 4 is block 104.
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

	for i := 0; i < len(ancestorCases); i++ {
		for b := ancestorCases[i][0]; b <= ancestorCases[i][1]; b++ {
			blockID := GetBlockID(b)
			height := ancestorCases[i][2]
			expectedAncestorID := GetBlockID(ancestorCases[i][3])

			getReq := types.GetBlocksByHeightReq{}
			getReq.HeadBlockID = blockID
			getReq.AncestorStartHeight = types.BlockHeightType(height)
			getReq.NumBlocks = 1
			getReq.ReturnBlockBlob = false
			getReq.ReturnReceiptBlob = false

			genericReq := types.BlockStoreReq{Value: &getReq}

			_, err := json.Marshal(genericReq)
			if err != nil {
				t.Error("Could not marshal JSON", err)
			}

			result, err := handler.HandleRequest(&genericReq)
			if err != nil {
				t.Error("Got error retrieving block:", err)
			}
			if result == nil {
				t.Error("Got nil result")
			}

			resp := result.Value.(types.GetBlocksByHeightResp)
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

			getReq := types.GetBlocksByHeightReq{}
			getReq.HeadBlockID = blockID
			getReq.NumBlocks = 1
			getReq.ReturnBlockBlob = false
			getReq.ReturnReceiptBlob = false

			// GetAncestorAtHeight where the requested height is equal to the height of the requested head
			getReq.AncestorStartHeight = types.BlockHeightType(height + 1)

			genericReq := types.BlockStoreReq{Value: &getReq}

			result, err := handler.HandleRequest(&genericReq)
			if err == nil {
				t.Error("Unexpectedly got non-error result attempting to retrieve descendant block:", result)
			}
			if _, ok := err.(*BlockHeightMismatch); !ok {
				t.Error("Err should be BlockHeightMismatch")
			}
			if err.Error() != "Block height mismatch" {
				t.Error("Unexpected error text")
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
				getReq := types.GetBlocksByHeightReq{}
				getReq.HeadBlockID = headID
				getReq.NumBlocks = types.UInt32(k - j)
				getReq.ReturnBlockBlob = false
				getReq.ReturnReceiptBlob = false
				getReq.AncestorStartHeight = types.BlockHeightType(j)

				genericReq := types.BlockStoreReq{Value: &getReq}

				result, err := handler.HandleRequest(&genericReq)
				if err != nil {
					t.Error("GetBlocksByHeightReq returned error: " + err.Error())
				}

				endIndex := k
				if endIndex > len(treeHist[i]) {
					endIndex = len(treeHist[i])
				}
				blockSeq := treeHist[i][j:endIndex]

				resp := result.Value.(types.GetBlocksByHeightResp)
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

func GetAddTransactionReq(n types.UInt64) types.AddTransactionReq {
	vb := n.Serialize(types.NewVariableBlob())
	m := types.Multihash{ID: 0x12, Digest: *vb}
	r := types.AddTransactionReq{TransactionID: m, TransactionBlob: *vb}
	return r
}

func GetGetTransactionsByIDReq(start uint64, num uint64) types.GetTransactionsByIDReq {
	vm := make([]types.Multihash, 0)
	for i := types.UInt64(start); i < types.UInt64(start+num); i++ {
		vb := i.Serialize(types.NewVariableBlob())
		m := types.Multihash{ID: 0x12, Digest: *vb}
		vm = append(vm, m)
	}

	r := types.GetTransactionsByIDReq{TransactionIds: vm}
	return r
}

type TxnErrorBackend struct {
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

// Put returns an error
func (backend *TxnBadBackend) Put(key []byte, value []byte) error {
	return nil
}

// Get gets an error
func (backend *TxnBadBackend) Get(key []byte) ([]byte, error) {
	return []byte{255, 255, 255, 255, 255}, nil
}

type TxnLongBackend struct {
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
	reqs := make([]types.AddTransactionReq, 32)
	for i := 0; i < 32; i++ {
		reqs[i] = GetAddTransactionReq(types.UInt64(i))
	}

	// Add the transactions
	for bType := range backendTypes {
		b := NewBackend(bType)
		handler := RequestHandler{b}
		for _, req := range reqs {
			bsr := types.BlockStoreReq{Value: &req}

			result, err := handler.HandleRequest(&bsr)
			if err != nil {
				t.Error("Got error adding transaction:", err)
			}
			if result == nil {
				t.Error("Got nil result")
			}
		}

		// Test adding an already existing transaction
		{
			bsr := types.BlockStoreReq{Value: &reqs[0]}
			result, err := handler.HandleRequest(&bsr)
			if err != nil {
				t.Error("Got error adding transaction:", err)
			}
			if result == nil {
				t.Error("Got nil result")
			}
		}

		// Test adding bad transaction
		{
			r := types.AddTransactionReq{TransactionID: reqs[0].TransactionID, TransactionBlob: nil}
			bsr := types.BlockStoreReq{Value: &r}
			_, err := handler.HandleRequest(&bsr)
			if _, ok := err.(*NilTransaction); !ok {
				t.Error("Nil transaction not returning correct error.")
			} else if err.Error() == "" {
				t.Error("Error incorrect message:", err)
			}
		}

		// Fetch the transactions
		{
			r := GetGetTransactionsByIDReq(0, 32)
			bsr := types.BlockStoreReq{Value: &r}
			result, err := handler.HandleRequest(&bsr)
			if err != nil {
				t.Error("Error fetching transactions:", err)
			}
			if result == nil {
				t.Error("Got nil result")
			}

			tres, ok := result.Value.(types.GetTransactionsByIDResp)
			if !ok {
				t.Error("Result is wrong type")
			}

			for i, nt := range tres.TransactionItems {
				if !bytes.Equal(reqs[i].TransactionBlob, nt.TransactionBlob) {
					t.Error("Result does not match added transaction")
				}
			}
		}

		// Test fetching an invalid transaction
		{
			r := GetGetTransactionsByIDReq(64, 1)
			bsr := types.BlockStoreReq{Value: &r}
			_, err := handler.HandleRequest(&bsr)
			if _, ok := err.(*TransactionNotPresent); !ok {
				t.Error("Did not recieve expected TransactionNotPresent error")
			} else if err.Error() == "" {
				t.Error("Error incorrect message:", err)
			}
		}

		CloseBackend(b)
	}

	// Test error on add
	{
		handler := RequestHandler{&TxnErrorBackend{}}
		r := GetAddTransactionReq(2)
		tr := types.BlockStoreReq{Value: &r}
		_, err := handler.HandleRequest(&tr)
		if err == nil {
			t.Error("Should have errored on transaction add, but did not")
		}
	}

	// Test error on get
	{
		handler := RequestHandler{&TxnErrorBackend{}}
		r := GetGetTransactionsByIDReq(0, 1)
		tr := types.BlockStoreReq{Value: &r}
		_, err := handler.HandleRequest(&tr)
		if err == nil {
			t.Error("Should have errored on transaction get, but did not")
		}
	}

	// Test bad record
	{
		handler := RequestHandler{&TxnBadBackend{}}
		r := GetGetTransactionsByIDReq(0, 1)
		tr := types.BlockStoreReq{Value: &r}
		_, err := handler.HandleRequest(&tr)
		if err == nil {
			t.Error("Should have errored on transaction get, but did not")
		}
	}

	// Test too long record
	{
		handler := RequestHandler{&TxnLongBackend{}}
		r := GetGetTransactionsByIDReq(0, 1)
		tr := types.BlockStoreReq{Value: &r}
		_, err := handler.HandleRequest(&tr)
		if err == nil {
			t.Error("Should have errored on transaction get, but did not")
		}
	}
}

func TestInternalError(t *testing.T) {
	err := InternalError{}
	if err.Error() != "Internal constraint was violated" {
		t.Error("Unexpected error text")
	}
}
