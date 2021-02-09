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

// NilTransaction is an error type for nil transactions
type NilTransaction struct {
}

func (e *NilTransaction) Error() string {
	return "Transaction blob is Nil"
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
	return types.NewGetBlocksByIDResp(), nil
}

func (handler *RequestHandler) handleGetBlocksByHeightReq(req *types.GetBlocksByHeightReq) (*types.GetBlocksByHeightResp, error) {
	if req.NumBlocks != 1 {
		return nil, &NotImplemented{}
	}
	if req.ReturnBlockBlob {
		return nil, &NotImplemented{}
	}
	if req.ReturnReceiptBlob {
		return nil, &NotImplemented{}
	}

	ancestorID, err := getAncestorIDAtHeight(handler.Backend, &req.HeadBlockID, req.AncestorStartHeight)
	if err != nil {
		return nil, err
	}

	resp := types.GetBlocksByHeightResp{}
	resp.BlockItems = types.VectorBlockItem(make([]types.BlockItem, 1))
	resp.BlockItems[0].BlockID = *ancestorID
	resp.BlockItems[0].BlockHeight = req.AncestorStartHeight
	resp.BlockItems[0].BlockBlob = []byte{}
	resp.BlockItems[0].BlockReceiptBlob = []byte{}

	return &resp, nil
}

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
 * Let a = GetPreviousHeights(h).
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
	record.BlockBlob = req.BlockToAdd.BlockBlob
	record.BlockReceiptBlob = req.BlockToAdd.BlockReceiptBlob

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
	if req.TransactionBlob == nil {
		return nil, &NilTransaction{}
	}

	record := types.TransactionRecord{}
	record.TransactionBlob = req.TransactionBlob

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
		resp.TransactionItems = append(resp.TransactionItems, types.TransactionItem{TransactionBlob: record.TransactionBlob})
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
