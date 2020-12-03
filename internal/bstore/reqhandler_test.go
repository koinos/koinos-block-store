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

	. "github.com/koinos/koinos-block-store/internal/types"
	types "github.com/koinos/koinos-block-store/internal/types"
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

		testReq := BlockStoreReq{Value: NewReservedReq()}
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
		CloseBackend(b)
	}
}

type UnknownReq struct {
}

func TestHandleUnknownRequestType(t *testing.T) {
	for bType := range backendTypes {
		b := NewBackend(bType)
		handler := RequestHandler{b}

		testReq := BlockStoreReq{Value: UnknownReq{}}
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
		yTest := GetPreviousHeights(x)

		if !SliceEqual(yRef, yTest) {
			t.Errorf("Testing %d, expected %v, got %v", x, yRef, yTest)
		}
	}
}

func GetBlockID(num uint64) Multihash {
	if num == 0 {
		return GetEmptyBlockID()
	}
	dataBytes := make([]byte, binary.MaxVarintLen64)
	count := binary.PutUvarint(dataBytes, num)

	hash := sha256.Sum256(dataBytes[:count])

	var vb VariableBlob = VariableBlob(hash[:])

	return Multihash{0x12, vb}
	// return Multihash{ 0x12, data_bytes[:count] }
}

func GetEmptyBlockID() Multihash {
	vb := VariableBlob(make([]byte, 32))
	return Multihash{0x12, vb}
}

func GetBlockBody(num uint64) VariableBlob {
	greetings := []string{
		"Hello this is block %d.",
		"Greetings from block %d.",
		"I like being in block %d.",
	}

	return []byte(fmt.Sprintf(greetings[int(num)%len(greetings)], num))
}

func TestAddBlocks(t *testing.T) {
	for bType := range backendTypes {
		b := NewBackend(bType)

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

		handler := RequestHandler{b}
		for i := 0; i < len(tree); i++ {
			for j := 1; j < len(tree[i]); j++ {
				blockID := GetBlockID(tree[i][j])
				parentID := GetBlockID(tree[i][j-1])

				// fmt.Printf("Block %d has ID %v\n", tree[i][j], hex.EncodeToString( block_id.Digest ) );
				addReq := AddBlockReq{}
				addReq.BlockToAdd.BlockID = blockID
				addReq.PreviousBlockID = parentID
				addReq.BlockToAdd.BlockHeight = BlockHeightType(tree[i][j] % 100)
				addReq.BlockToAdd.BlockBlob = GetBlockBody(tree[i][j])
				addReq.BlockToAdd.BlockReceiptBlob = VariableBlob(make([]byte, 0))

				genericReq := BlockStoreReq{Value: &addReq}

				json, err := json.Marshal(genericReq)
				if err != nil {
					t.Error("Could not marshal JSON", err)
				}
				fmt.Printf("%s\n", string(json))

				result, err := handler.HandleRequest(&genericReq)
				if err != nil {
					t.Error("Got error adding block:", err)
				}
				if result == nil {
					t.Error("Got nil result")
				}
			}
		}

		// Item {105, 120, 4, 104} means for blocks 105-120, the ancestor at height 4 is block 104.
		ancestorCases := [][]uint64{
			{102, 120, 1, 101}, {103, 120, 2, 102}, {104, 120, 3, 103}, {105, 120, 4, 104},
			{106, 120, 5, 105}, {107, 120, 6, 106}, {108, 120, 7, 107}, {109, 120, 8, 108}, {110, 120, 9, 109},
			{111, 120, 10, 110}, {112, 120, 11, 111}, {113, 120, 12, 112}, {114, 120, 13, 113}, {115, 120, 14, 114},
			{116, 120, 15, 115}, {117, 120, 16, 116}, {118, 120, 17, 117}, {119, 120, 18, 118}, {120, 120, 19, 119},

			{204, 211, 1, 101}, {204, 211, 2, 102}, {204, 211, 3, 103}, {205, 211, 4, 204},
			{206, 211, 5, 205}, {207, 211, 6, 206}, {208, 211, 7, 207}, {209, 211, 8, 208}, {210, 211, 9, 209},
			{211, 211, 10, 210},

			{304, 307, 1, 101}, {304, 307, 2, 102}, {304, 307, 3, 103}, {305, 307, 4, 304},
			{306, 307, 5, 305}, {307, 307, 6, 306},

			{407, 419, 1, 101}, {407, 419, 2, 102}, {407, 419, 3, 103}, {407, 419, 4, 104},
			{407, 419, 5, 105}, {407, 419, 6, 106}, {408, 419, 7, 407}, {409, 419, 8, 408}, {410, 419, 9, 409},
			{411, 419, 10, 410}, {412, 419, 11, 411}, {413, 419, 12, 412}, {414, 419, 13, 413}, {415, 419, 14, 414},
			{416, 419, 15, 415}, {417, 419, 16, 416}, {418, 419, 17, 417}, {419, 419, 18, 418},

			{510, 511, 1, 101}, {510, 511, 2, 102}, {510, 511, 3, 103}, {510, 511, 4, 104},
			{510, 511, 5, 105}, {510, 511, 6, 106}, {510, 511, 7, 107}, {510, 511, 8, 108}, {510, 511, 9, 109},
			{511, 511, 10, 510},

			{613, 614, 1, 101}, {613, 614, 2, 102}, {613, 614, 3, 103}, {613, 614, 4, 104},
			{613, 614, 5, 105}, {613, 614, 6, 106}, {613, 614, 7, 107}, {613, 614, 8, 108}, {613, 614, 9, 109},
			{613, 614, 10, 110}, {613, 614, 11, 111}, {613, 614, 12, 112}, {614, 614, 13, 613},

			{712, 718, 1, 101}, {712, 718, 2, 102}, {712, 718, 3, 103}, {712, 718, 4, 104},
			{712, 718, 5, 105}, {712, 718, 6, 106}, {712, 718, 7, 407}, {712, 718, 8, 408}, {712, 718, 9, 409},
			{712, 718, 10, 410}, {712, 718, 11, 411}, {713, 718, 12, 712}, {714, 718, 13, 713}, {715, 718, 14, 714},
			{716, 718, 15, 715}, {717, 718, 16, 716}, {718, 718, 17, 717},

			{815, 819, 1, 101}, {815, 819, 2, 102}, {815, 819, 3, 103}, {815, 819, 4, 104},
			{815, 819, 5, 105}, {815, 819, 6, 106}, {815, 819, 7, 407}, {815, 819, 8, 408}, {815, 819, 9, 409},
			{815, 819, 10, 410}, {815, 819, 11, 411}, {815, 819, 12, 712}, {815, 819, 13, 713}, {815, 819, 14, 714},
			{816, 819, 15, 815}, {817, 819, 16, 816}, {818, 819, 17, 817}, {819, 819, 18, 818},
		}

		for i := 0; i < len(ancestorCases); i++ {
			for b := ancestorCases[i][0]; b <= ancestorCases[i][1]; b++ {
				blockID := GetBlockID(b)
				height := ancestorCases[i][2]
				expectedAncestorID := GetBlockID(ancestorCases[i][3])

				getReq := GetBlocksByHeightReq{}
				getReq.HeadBlockID = blockID
				getReq.AncestorStartHeight = BlockHeightType(height)
				getReq.NumBlocks = 1
				getReq.ReturnBlockBlob = false
				getReq.ReturnReceiptBlob = false

				genericReq := BlockStoreReq{Value: &getReq}

				json, err := json.Marshal(genericReq)
				if err != nil {
					t.Error("Could not marshal JSON", err)
				}
				fmt.Printf("%s\n", string(json))

				result, err := handler.HandleRequest(&genericReq)
				if err != nil {
					t.Error("Got error retrieving block:", err)
				}
				if result == nil {
					t.Error("Got nil result")
				}

				resp := result.Value.(GetBlocksByHeightResp)
				if len(resp.BlockItems) != 1 {
					t.Error("Expected result of length 1")
				}

				if resp.BlockItems[0].BlockHeight != BlockHeightType(height) {
					t.Errorf("Unexpected ancestor height:  Got %d, expected %d", resp.BlockItems[0].BlockHeight, height)
				}

				if !resp.BlockItems[0].BlockID.Equals(&expectedAncestorID) {
					t.Error("Unexpected ancestor block ID")
				}
			}
		}
		CloseBackend(b)
	}
}

func GetAddTransactionReq(n UInt64) AddTransactionReq {
	vb := n.Serialize(types.NewVariableBlob())
	m := Multihash{ID: 0x12, Digest: *vb}
	r := types.AddTransactionReq{TransactionID: m, TransactionBlob: *vb}
	return r
}

func GetGetTransactionsByIdReq(start uint64, num uint64) GetTransactionsByIDReq {
	vm := make([]Multihash, 0)
	for i := UInt64(start); i < UInt64(start+num); i++ {
		vb := i.Serialize(types.NewVariableBlob())
		m := Multihash{ID: 0x12, Digest: *vb}
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
		reqs[i] = GetAddTransactionReq(UInt64(i))
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
			r := GetGetTransactionsByIdReq(0, 32)
			bsr := types.BlockStoreReq{Value: &r}
			result, err := handler.HandleRequest(&bsr)
			if err != nil {
				t.Error("Error fetching transactions:", err)
			}
			if result == nil {
				t.Error("Got nil result")
			}

			tres, ok := result.Value.(GetTransactionsByIDResp)
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
			r := GetGetTransactionsByIdReq(64, 1)
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
		r := GetGetTransactionsByIdReq(0, 1)
		tr := types.BlockStoreReq{Value: &r}
		_, err := handler.HandleRequest(&tr)
		if err == nil {
			t.Error("Should have errored on transaction get, but did not")
		}
	}

	// Test bad record
	{
		handler := RequestHandler{&TxnBadBackend{}}
		r := GetGetTransactionsByIdReq(0, 1)
		tr := types.BlockStoreReq{Value: &r}
		_, err := handler.HandleRequest(&tr)
		if err == nil {
			t.Error("Should have errored on transaction get, but did not")
		}
	}

	// Test too long record
	{
		handler := RequestHandler{&TxnLongBackend{}}
		r := GetGetTransactionsByIdReq(0, 1)
		tr := types.BlockStoreReq{Value: &r}
		_, err := handler.HandleRequest(&tr)
		if err == nil {
			t.Error("Should have errored on transaction get, but did not")
		}
	}
}
