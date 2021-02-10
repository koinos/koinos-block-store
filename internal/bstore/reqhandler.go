package bstore

import (
	"encoding/hex"
	"fmt"
	"math/bits"

	types "github.com/koinos/koinos-types-golang"
)

// RequestHandler contains a backend object and handles requests
type RequestHandler struct {
	Backend BlockStoreBackend
}

// ReservedReqError is an error type that is thrown when a reserved request is passed to the request handler
type ReservedReqError struct {
}

func (e *ReservedReqError) Error() string {
	return "Reserved request is not supported"
}

// UnknownReqError is an error that is thrown when an unknown request is given to the request handler
type UnknownReqError struct {
}

func (e *UnknownReqError) Error() string {
	return "Unknown request type"
}

// InternalError is an error type that is thrown when an internal constraint is violated
type InternalError struct {
}

func (e *InternalError) Error() string {
	return "Internal constraint was violated"
}

// BlockNotPresent is an error type thrown when asking for a block that is not contained in the blockstore
type BlockNotPresent struct {
}

func (e *BlockNotPresent) Error() string {
	return "Block was not present"
}

// TransactionNotPresent is an error type thrown when asking for a transaction that is not contained in the blockstore
type TransactionNotPresent struct {
}

func (e *TransactionNotPresent) Error() string {
	return "Transaction was not present"
}

// DeserializeError is an error type for errors during deserialization
type DeserializeError struct {
}

func (e *DeserializeError) Error() string {
	return "Could not deserialize block"
}

// UnexpectedHeightError is an error type for bad block heights
type UnexpectedHeightError struct {
}

func (e *UnexpectedHeightError) Error() string {
	return "Unexpected height (corrupt block store?)"
}

// TraverseBeforeGenesisError is an error type when the blockchain attempts to traverse before genesis
type TraverseBeforeGenesisError struct {
}

func (e *TraverseBeforeGenesisError) Error() string {
	return "Attempt to traverse before genesis"
}

// NotImplemented is an error type for unimplemented types
type NotImplemented struct {
}

func (e *NotImplemented) Error() string {
	return "Unimplemented case"
}

// BlockHeightMismatch is an error type thrown when querying ancestor of block B at height H where H >= B.height.
type BlockHeightMismatch struct {
}

func (e *BlockHeightMismatch) Error() string {
	return "Block height mismatch"
}

func (handler *RequestHandler) handleReservedReq(req *types.ReservedReq) (*types.ReservedResp, error) {
	return nil, &ReservedReqError{}
}

func (handler *RequestHandler) handleGetBlocksByIDReq(req *types.GetBlocksByIDReq) (*types.GetBlocksByIDResp, error) {
	// TODO implement this
	return types.NewGetBlocksByIDResp(), nil
}

/**
 * Internal helper method to fill blocks.
 *
 * Given a block ID and height, return the block and the previous numBlocks-1 blocks.
 * Return empty block if we go past the beginning.
 */
func (handler *RequestHandler) fillBlocks(
	lastID *types.Multihash,
	numBlocks types.UInt32,
	returnBlockBlob types.Boolean,
	returnReceiptBlob types.Boolean) (types.VectorBlockItem, error) {

	blockItems := types.VectorBlockItem(make([]types.BlockItem, numBlocks))

	if numBlocks <= 0 {
		return blockItems, nil
	}

	blockID := *lastID

	var i types.UInt32
	for i = 0; i < numBlocks; i++ {
		// k is the index into the array
		k := numBlocks - i - 1

		vbKey := blockID.Serialize(types.NewVariableBlob())
		recordBytes, err := handler.Backend.Get(*vbKey)
		if err != nil {
			return nil, err
		}
		if len(recordBytes) == 0 {
			// If block does not exist, return a default-initialized block.
			// TODO:  What is the canonical zero block ID?
			continue
		}

		// TODO is there a way to avoid this copy?
		var vbValue types.VariableBlob = types.VariableBlob(recordBytes)

		consumed, record, err := types.DeserializeBlockRecord(&vbValue)
		if err != nil {
			fmt.Println("Couldn't deserialize block record")
			fmt.Println("vb: ", recordBytes)
			return nil, err
		}
		if consumed != uint64(len(recordBytes)) {
			return nil, &DeserializeError{}
		}

		// Blocks are expected to have decreasing height
		if i > 0 {
			expectedHeight := blockItems[k+1].BlockHeight - 1
			if record.BlockHeight != expectedHeight {
				fmt.Println("record height:", record.BlockHeight)
				fmt.Println("expect height:", expectedHeight)
				return nil, &UnexpectedHeightError{}
			}
		}

		blockItems[k].BlockID = blockID
		blockItems[k].BlockHeight = record.BlockHeight
		if returnBlockBlob {
			blockItems[k].Block = record.Block
		}
		if returnReceiptBlob {
			blockItems[k].BlockReceipt = record.BlockReceipt
		}

		if (record.BlockHeight == 0) || (len(record.PreviousBlockIds) == 0) {
			return nil, &TraverseBeforeGenesisError{}
		}

		blockID = record.PreviousBlockIds[0]
	}

	return blockItems, nil
}

func (handler *RequestHandler) handleGetBlocksByHeightReq(req *types.GetBlocksByHeightReq) (*types.GetBlocksByHeightResp, error) {
	resp := types.NewGetBlocksByHeightResp()

	if req.NumBlocks <= 0 {
		return resp, nil
	}

	headBlockHeight, err := getBlockHeight(handler.Backend, &req.HeadBlockID)
	if err != nil {
		return nil, err
	}

	if req.AncestorStartHeight > headBlockHeight {
		return nil, &BlockHeightMismatch{}
	}

	numBlocks := req.NumBlocks
	endHeight := uint64(req.AncestorStartHeight) + uint64(numBlocks)
	if endHeight > uint64(headBlockHeight) {
		endHeight = uint64(headBlockHeight) + 1
		numBlocks = types.UInt32(endHeight - uint64(req.AncestorStartHeight))
	}

	blockID, err := getAncestorIDAtHeight(handler.Backend, &req.HeadBlockID, types.BlockHeightType(endHeight-1))
	if err != nil {
		if _, ok := err.(*BlockHeightMismatch); !ok {
			return nil, err
		}
	}

	blockItems, err := handler.fillBlocks(blockID, numBlocks, req.ReturnBlock, req.ReturnReceipt)
	if err != nil {
		return nil, err
	}

	resp.BlockItems = blockItems

	if len(resp.BlockItems) > 0 {
		expectedHeight := req.AncestorStartHeight
		if resp.BlockItems[0].BlockHeight != expectedHeight {
			fmt.Println("start  height:", resp.BlockItems[0].BlockHeight)
			fmt.Println("expect height:", expectedHeight)
			return nil, &UnexpectedHeightError{}
		}
	}

	return resp, nil
}

/**
 * Compute the array of previous heights for a given height.
 *
 * This is a helper function used to implement the skip-list indexing scheme used by getAncestorIDAtHeight().
 *
 * - Block n has a pointer to block n-1.
 * - If 2 | n, block n has a pointer to block n-2.
 * - If 4 | n, block n has a pointer to block n-4.
 * - If 8 | n, block n has a pointer to block n-8.
 *
 * In general, if 2^k | n, then getPreviousHeights(n)[k] = n-2^k.
 *
 * This has the following properties:
 *
 * - On average, each block has a pointer to 2 previous blocks, so average-case use O(1) storage per block.
 * - The path connecting any two blocks is at most O(log(dh)) where dh is the difference in block heights.
 *
 */
func getPreviousHeights(x uint64) []uint64 {
	// TODO:  Do we want to subtract 1 from the input and add 1 to the output, to account for the fact that initial block's height is 1?
	if x == 0 {
		return []uint64{}
	}

	zeros := bits.TrailingZeros64(x)
	result := make([]uint64, zeros+1)
	for i := 0; i <= zeros; i++ {
		result[i] = x - (uint64(1) << i)
	}

	return result
}

/**
 * Get the index into the previous height array when searching for the given height.
 *
 * Let a = getPreviousHeights(h).
 * Let i, y, err = getPreviousHeightIndex(goal, h, x).
 * Then i is the greatest integer that satisfies a[i] >= h, and y is a[i].
 *
 * This method could be implemented by scanning the result of GetPreviousHeights().  But as an
 * optimization, it separately implements the same algorithm as GetPreviousHeights() to avoid
 * allocating memory.
 */
func getPreviousHeightIndex(goal types.BlockHeightType, current types.BlockHeightType) (int, types.BlockHeightType, error) {
	if goal >= current {
		return 0, 0, &BlockHeightMismatch{}
	}

	var x uint64 = uint64(current)
	var g uint64 = uint64(goal)
	zeros := bits.TrailingZeros64(x)

	var lastH uint64 = 0
	for i := 0; i <= zeros; i++ {
		h := x - (uint64(1) << i)
		if h < g {
			return i - 1, types.BlockHeightType(lastH), nil
		}
		lastH = h
	}
	return zeros, types.BlockHeightType(lastH), nil
}

/**
 * Fetch a block by ID and then return its height.
 */
func getBlockHeight(backend BlockStoreBackend, blockID *types.Multihash) (types.BlockHeightType, error) {
	vbKey := blockID.Serialize(types.NewVariableBlob())

	recordBytes, err := backend.Get(*vbKey)
	if err != nil {
		return 0, err
	}
	if len(recordBytes) == 0 {
		return 0, &BlockNotPresent{}
	}

	// TODO is there a way to avoid this copy?
	var vbValue types.VariableBlob = types.VariableBlob(recordBytes)

	consumed, record, err := types.DeserializeBlockRecord(&vbValue)
	if err != nil {
		fmt.Println("Couldn't deserialize block record")
		fmt.Println("vb: ", recordBytes)
		return 0, err
	}
	if consumed != uint64(len(recordBytes)) {
		return 0, &DeserializeError{}
	}

	return record.BlockHeight, nil
}

func getAncestorIDAtHeight(backend BlockStoreBackend, blockID *types.Multihash, height types.BlockHeightType) (*types.Multihash, error) {

	var expectedHeight types.BlockHeightType
	var hasExpectedHeight bool = false

	for {
		vbKey := blockID.Serialize(types.NewVariableBlob())

		recordBytes, err := backend.Get(*vbKey)
		if err != nil {
			return nil, err
		}
		if len(recordBytes) == 0 {
			return nil, &BlockNotPresent{}
		}

		// TODO is there a way to avoid this copy?
		var vbValue types.VariableBlob = types.VariableBlob(recordBytes)

		consumed, record, err := types.DeserializeBlockRecord(&vbValue)
		if err != nil {
			fmt.Println("Couldn't deserialize block record")
			fmt.Println("vb: ", recordBytes)
			return nil, err
		}
		if consumed != uint64(len(recordBytes)) {
			return nil, &DeserializeError{}
		}
		if hasExpectedHeight && (record.BlockHeight != expectedHeight) {
			fmt.Println("record height:", record.BlockHeight)
			fmt.Println("expect height:", expectedHeight)
			return nil, &UnexpectedHeightError{}
		}

		if record.BlockHeight == height {
			return &record.BlockID, nil
		}

		newIndex, newHeight, err := getPreviousHeightIndex(height, record.BlockHeight)
		if err != nil {
			return nil, err
		}
		if newIndex >= len(record.PreviousBlockIds) {
			return nil, &UnexpectedHeightError{}
		}

		// We only care about the ID, so once we've found it in a previous list, no need to actually fetch the record
		blockID = &record.PreviousBlockIds[newIndex]
		if newHeight == height {
			return blockID, nil
		}
		expectedHeight = newHeight
		hasExpectedHeight = true
	}
}

func (handler *RequestHandler) handleAddBlockReq(req *types.AddBlockReq) (*types.AddBlockResp, error) {

	record := types.BlockRecord{}

	record.BlockID = req.BlockToAdd.BlockID
	record.BlockHeight = req.BlockToAdd.BlockHeight
	record.Block = req.BlockToAdd.Block
	record.BlockReceipt = req.BlockToAdd.BlockReceipt

	if req.BlockToAdd.BlockHeight > 0 {
		previousHeights := getPreviousHeights(uint64(req.BlockToAdd.BlockHeight))

		record.PreviousBlockIds = make([]types.Multihash, len(previousHeights))

		for i := 0; i < len(previousHeights); i++ {
			h := previousHeights[i]
			if h >= uint64(record.BlockHeight) {
				return nil, &InternalError{}
			} else if h == uint64(record.BlockHeight)-1 {
				record.PreviousBlockIds[i] = req.PreviousBlockID
			} else {
				previousID, err := getAncestorIDAtHeight(handler.Backend, &req.PreviousBlockID, types.BlockHeightType(h))
				if err != nil {
					return nil, err
				}
				record.PreviousBlockIds[i] = *previousID
			}
		}
	} else {
		record.PreviousBlockIds = make([]types.Multihash, 0)
	}

	vbKey := record.BlockID.Serialize(types.NewVariableBlob())
	vbValue := record.Serialize(types.NewVariableBlob())

	err := handler.Backend.Put(*vbKey, *vbValue)
	if err != nil {
		return nil, err
	}

	resp := types.AddBlockResp{}
	return &resp, nil
}

func (handler *RequestHandler) handleAddTransactionReq(req *types.AddTransactionReq) (*types.AddTransactionResp, error) {
	record := types.TransactionRecord{}
	record.Transaction = req.Transaction

	vbKey := req.TransactionID.Serialize(types.NewVariableBlob())
	vbValue := record.Serialize(types.NewVariableBlob())

	err := handler.Backend.Put(*vbKey, *vbValue)
	if err != nil {
		return nil, err
	}

	resp := types.AddTransactionResp{}
	return &resp, nil
}

func (handler *RequestHandler) handleGetTransactionsByIDReq(req *types.GetTransactionsByIDReq) (*types.GetTransactionsByIDResp, error) {
	resp := types.GetTransactionsByIDResp{}
	resp.TransactionItems = types.VectorTransactionItem(make([]types.TransactionItem, 0))

	for _, tid := range req.TransactionIds {
		vbKey := tid.Serialize(types.NewVariableBlob())

		recordBytes, err := handler.Backend.Get(*vbKey)
		if err != nil {
			return nil, err
		}
		if len(recordBytes) == 0 {
			fmt.Println("Transaction not present, key is", hex.EncodeToString(tid.Digest))
			return nil, &TransactionNotPresent{}
		}

		vbValue := types.VariableBlob(recordBytes)
		consumed, record, err := types.DeserializeTransactionRecord(&vbValue)
		if err != nil {
			fmt.Println("Couldn't deserialize transaction record")
			fmt.Println("vb: ", recordBytes)
			return nil, err
		}
		if consumed != uint64(len(recordBytes)) {
			return nil, &DeserializeError{}
		}
		resp.TransactionItems = append(resp.TransactionItems, types.TransactionItem{Transaction: record.Transaction})
	}

	return &resp, nil
}

// HandleRequest handles and routes blockstore requests
func (handler *RequestHandler) HandleRequest(req *types.BlockStoreReq) (*types.BlockStoreResp, error) {
	switch req.Value.(type) {
	case *types.ReservedReq:
		v := req.Value.(*types.ReservedReq)
		result, err := handler.handleReservedReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: result}, nil
	case *types.GetBlocksByIDReq:
		v := req.Value.(*types.GetBlocksByIDReq)
		result, err := handler.handleGetBlocksByIDReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: result}, nil
	case *types.GetBlocksByHeightReq:
		v := req.Value.(*types.GetBlocksByHeightReq)
		result, err := handler.handleGetBlocksByHeightReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: result}, nil
	case *types.AddBlockReq:
		v := req.Value.(*types.AddBlockReq)
		result, err := handler.handleAddBlockReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: result}, nil
	case *types.AddTransactionReq:
		v := req.Value.(*types.AddTransactionReq)
		result, err := handler.handleAddTransactionReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: result}, nil
	case *types.GetTransactionsByIDReq:
		v := req.Value.(*types.GetTransactionsByIDReq)
		result, err := handler.handleGetTransactionsByIDReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: result}, nil
	}
	return nil, &UnknownReqError{}
}
