package bstore

import (
	"encoding/hex"
	"fmt"
	"math/bits"

	types "github.com/koinos/koinos-block-store/internal/types"
)

type RequestHandler struct {
	Backend BlockStoreBackend
}

type ReservedReqError struct {
}

func (e *ReservedReqError) Error() string {
	return "Reserved request is not supported"
}

type UnknownReqError struct {
}

func (e *UnknownReqError) Error() string {
	return "Unknown request type"
}

type InternalError struct {
}

func (e *InternalError) Error() string {
	return "Internal constraint was violated"
}

type BlockNotPresent struct {
}

func (e *BlockNotPresent) Error() string {
	return "Block was not present"
}

type TransactionNotPresent struct {
}

func (e *TransactionNotPresent) Error() string {
	return "Transaction was not present"
}

type NilTransaction struct {
}

func (e *NilTransaction) Error() string {
	return "Transaction blob is Nil"
}

type DeserializeError struct {
}

func (e *DeserializeError) Error() string {
	return "Could not deserialize block"
}

type UnexpectedHeightError struct {
}

func (e *UnexpectedHeightError) Error() string {
	return "Unexpected height (corrupt block store?)"
}

type NotImplemented struct {
}

func (e *NotImplemented) Error() string {
	return "Unimplemented case"
}

/**
 * Thrown when querying ancestor of block B at height H where H >= B.height.
 */
type BlockHeightMismatch struct {
}

func (e *BlockHeightMismatch) Error() string {
	return "Block height mismatch"
}

func (handler *RequestHandler) HandleReservedReq(req *types.ReservedReq) (*types.ReservedResp, error) {
	return nil, &ReservedReqError{}
}

func (handler *RequestHandler) HandleGetBlocksByIdReq(req *types.GetBlocksByIDReq) (*types.GetBlocksByIDResp, error) {
	return types.NewGetBlocksByIDResp(), nil
}

func (handler *RequestHandler) HandleGetBlocksByHeightReq(req *types.GetBlocksByHeightReq) (*types.GetBlocksByHeightResp, error) {
	if req.NumBlocks != 1 {
		return nil, &NotImplemented{}
	}
	if req.ReturnBlockBlob {
		return nil, &NotImplemented{}
	}
	if req.ReturnReceiptBlob {
		return nil, &NotImplemented{}
	}

	ancestor_id, err := GetAncestorIdAtHeight(handler.Backend, &req.HeadBlockID, req.AncestorStartHeight)
	if err != nil {
		return nil, err
	}

	resp := types.GetBlocksByHeightResp{}
	resp.BlockItems = types.VectorBlockItem(make([]types.BlockItem, 1))
	resp.BlockItems[0].BlockID = *ancestor_id
	resp.BlockItems[0].BlockHeight = req.AncestorStartHeight
	resp.BlockItems[0].BlockBlob = []byte{}
	resp.BlockItems[0].BlockReceiptBlob = []byte{}

	return &resp, nil
}

func GetPreviousHeights(x uint64) []uint64 {
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
 * Let i, y, err = GetPreviousHeightIndex(goal, h, x).
 * Then i is the greatest integer that satisfies a[i] >= h, and y is a[i].
 *
 * This method could be implemented by scanning the result of GetPreviousHeights().  But as an
 * optimization, it separately implements the same algorithm as GetPreviousHeights() to avoid
 * allocating memory.
 */
func GetPreviousHeightIndex(goal types.BlockHeightType, current types.BlockHeightType) (int, types.BlockHeightType, error) {
	if goal >= current {
		return 0, 0, &BlockHeightMismatch{}
	}

	var x uint64 = uint64(current)
	var g uint64 = uint64(goal)
	zeros := bits.TrailingZeros64(x)

	var last_h uint64 = 0
	for i := 0; i <= zeros; i++ {
		h := x - (uint64(1) << i)
		if h < g {
			return i - 1, types.BlockHeightType(last_h), nil
		}
		last_h = h
	}
	return zeros, types.BlockHeightType(last_h), nil
}

func GetAncestorIdAtHeight(backend BlockStoreBackend, block_id *types.Multihash, height types.BlockHeightType) (*types.Multihash, error) {

	var expected_height types.BlockHeightType
	var has_expected_height bool = false

	for {
		vb_key := block_id.Serialize(types.NewVariableBlob())

		record_bytes, err := backend.Get(*vb_key)
		if err != nil {
			return nil, err
		}
		if record_bytes == nil {
			fmt.Println("Block not present, key is", hex.EncodeToString(block_id.Digest))
			return nil, &BlockNotPresent{}
		}

		// TODO is there a way to avoid this copy?
		var vb_value types.VariableBlob = types.VariableBlob(record_bytes)

		consumed, record, err := types.DeserializeBlockRecord(&vb_value)
		if err != nil {
			fmt.Println("Couldn't deserialize block record")
			fmt.Println("vb: ", record_bytes)
			return nil, err
		}
		if consumed != uint64(len(record_bytes)) {
			return nil, &DeserializeError{}
		}
		if has_expected_height && (record.BlockHeight != expected_height) {
			fmt.Println("record height:", record.BlockHeight)
			fmt.Println("expect height:", expected_height)
			return nil, &UnexpectedHeightError{}
		}

		if record.BlockHeight == height {
			return &record.BlockID, nil
		}

		new_index, new_height, err := GetPreviousHeightIndex(height, record.BlockHeight)
		if err != nil {
			return nil, err
		}
		if new_index >= len(record.PreviousBlockIds) {
			return nil, &UnexpectedHeightError{}
		}

		// We only care about the ID, so once we've found it in a previous list, no need to actually fetch the record
		block_id = &record.PreviousBlockIds[new_index]
		if new_height == height {
			return block_id, nil
		}
		expected_height = new_height
		has_expected_height = true
	}
}

func MultihashIsZero(mh *types.Multihash) bool {
	for i := 0; i < len(mh.Digest); i++ {
		if mh.Digest[i] != 0 {
			return false
		}
	}
	return true
}

func (handler *RequestHandler) HandleAddBlockReq(req *types.AddBlockReq) (*types.AddBlockResp, error) {

	record := types.BlockRecord{}

	record.BlockID = req.BlockToAdd.BlockID
	record.BlockHeight = req.BlockToAdd.BlockHeight
	record.BlockBlob = req.BlockToAdd.BlockBlob
	record.BlockReceiptBlob = req.BlockToAdd.BlockReceiptBlob

	if req.BlockToAdd.BlockHeight > 0 {
		previous_heights := GetPreviousHeights(uint64(req.BlockToAdd.BlockHeight))

		record.PreviousBlockIds = make([]types.Multihash, len(previous_heights))

		for i := 0; i < len(previous_heights); i++ {
			h := previous_heights[i]
			if h >= uint64(record.BlockHeight) {
				return nil, &InternalError{}
			} else if h == uint64(record.BlockHeight)-1 {
				record.PreviousBlockIds[i] = req.PreviousBlockID
			} else {
				previous_id, err := GetAncestorIdAtHeight(handler.Backend, &req.PreviousBlockID, types.BlockHeightType(h))
				if err != nil {
					return nil, err
				}
				record.PreviousBlockIds[i] = *previous_id
			}
		}
	} else {
		record.PreviousBlockIds = make([]types.Multihash, 0)
	}

	vb_key := record.BlockID.Serialize(types.NewVariableBlob())
	vb_value := record.Serialize(types.NewVariableBlob())

	err := handler.Backend.Put(*vb_key, *vb_value)
	if err != nil {
		return nil, err
	}

	resp := types.AddBlockResp{}
	return &resp, nil
}

// HandleAddTransactionReq handles requests to add transactions to the blockstore
func (handler *RequestHandler) HandleAddTransactionReq(req *types.AddTransactionReq) (*types.AddTransactionResp, error) {
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

// HandleGetTransactionsByIdReq handles requests to fetch transactions from the blockstore
func (handler *RequestHandler) HandleGetTransactionsByIdReq(req *types.GetTransactionsByIDReq) (*types.GetTransactionsByIDResp, error) {
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

func (handler *RequestHandler) HandleRequest(req *types.BlockStoreReq) (*types.BlockStoreResp, error) {
	switch req.Value.(type) {
	case *types.ReservedReq:
		v := req.Value.(*types.ReservedReq)
		result, err := handler.HandleReservedReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: *result}, nil
	case *types.GetBlocksByIDReq:
		v := req.Value.(*types.GetBlocksByIDReq)
		result, err := handler.HandleGetBlocksByIdReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: *result}, nil
	case *types.GetBlocksByHeightReq:
		v := req.Value.(*types.GetBlocksByHeightReq)
		result, err := handler.HandleGetBlocksByHeightReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: *result}, nil
	case *types.AddBlockReq:
		v := req.Value.(*types.AddBlockReq)
		result, err := handler.HandleAddBlockReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: *result}, nil
	case *types.AddTransactionReq:
		v := req.Value.(*types.AddTransactionReq)
		result, err := handler.HandleAddTransactionReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: *result}, nil
	case *types.GetTransactionsByIDReq:
		v := req.Value.(*types.GetTransactionsByIDReq)
		result, err := handler.HandleGetTransactionsByIdReq(v)
		if err != nil {
			return nil, err
		}
		return &types.BlockStoreResp{Value: *result}, nil
	}
	return nil, &UnknownReqError{}
}
