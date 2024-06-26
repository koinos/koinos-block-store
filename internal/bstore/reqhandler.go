package bstore

import (
	"errors"
	"fmt"
	"math/bits"

	base58 "github.com/btcsuite/btcutil/base58"
	log "github.com/koinos/koinos-log-golang"
	types "github.com/koinos/koinos-types-golang"
)

const (
	highestBlockKey = 0x01
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
	blockID types.Multihash
}

func (e *BlockNotPresent) Error() string {
	return fmt.Sprintf("Block not present - Digest: %s, ID: %d", base58.Encode(e.blockID.Digest), e.blockID.ID)
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

// GetBlocksByID returns blocks by block ID
func (handler *RequestHandler) GetBlocksByID(req *types.GetBlocksByIDRequest) (*types.GetBlocksByIDResponse, error) {
	result := types.NewGetBlocksByIDResponse()

	result.BlockItems = make(types.VectorBlockItem, len(req.BlockID))

	for i := range req.BlockID {
		vbKey := req.BlockID[i].Serialize(types.NewVariableBlob())
		bytes, err := handler.Backend.Get([]byte(*vbKey))

		result.BlockItems[i].Block = *types.NewOptionalBlock()
		result.BlockItems[i].BlockReceipt = *types.NewOptionalBlockReceipt()

		if err != nil {
			continue
		}

		vbValue := types.VariableBlob(bytes)
		read, record, err := types.DeserializeBlockRecord(&vbValue)

		if read == 0 || err != nil {
			continue
		}

		result.BlockItems[i].BlockID = record.BlockID
		result.BlockItems[i].BlockHeight = record.BlockHeight

		if req.ReturnBlockBlob {
			result.BlockItems[i].Block = *types.NewOptionalBlock()
			result.BlockItems[i].Block.Value = &record.Block
		}

		if req.ReturnReceiptBlob {
			// TODO: Internally, Block Receipt needs to change
			// result.BlockItems[i].BlockReceipt = record.BlockReceipt
		}
	}

	return result, nil
}

/**
 * Internal helper method to fill blocks.
 *
 * Given a block ID and height, return the block and the previous numBlocks-1 blocks.
 * Return empty block if we go past the beginning.
 */
func (handler *RequestHandler) fillBlocks(
	lastID types.Multihash,
	numBlocks types.UInt32,
	returnBlock types.Boolean,
	returnReceipt types.Boolean) (types.VectorBlockItem, error) {

	blockItems := types.VectorBlockItem(make([]types.BlockItem, numBlocks))

	if numBlocks <= 0 {
		return blockItems, nil
	}

	//blockID := *lastID

	var i types.UInt32
	for i = 0; i < numBlocks; i++ {
		// k is the index into the array
		k := numBlocks - i - 1

		blockItems[k].Block = *types.NewOptionalBlock()
		blockItems[k].BlockReceipt = *types.NewOptionalBlockReceipt()

		vbKey := lastID.Serialize(types.NewVariableBlob())
		recordBytes, err := handler.Backend.Get(*vbKey)
		if err != nil {
			return nil, err
		}
		if len(recordBytes) == 0 {
			// If block does not exist, return a default-initialized block.
			continue
		}

		// TODO is there a way to avoid this copy?
		var vbValue types.VariableBlob = types.VariableBlob(recordBytes)

		consumed, record, err := types.DeserializeBlockRecord(&vbValue)
		if err != nil {
			log.Warn("Couldn't deserialize block record")
			log.Warnf("vb: %v", recordBytes)
			return nil, err
		}
		if consumed != uint64(len(recordBytes)) {
			return nil, &DeserializeError{}
		}

		// Blocks are expected to have decreasing height
		if i > 0 {
			expectedHeight := blockItems[k+1].BlockHeight - 1
			if record.BlockHeight != expectedHeight {
				log.Warnf("record height: %d", record.BlockHeight)
				log.Warnf("expect height: %d", expectedHeight)
				return nil, &UnexpectedHeightError{}
			}
		}

		blockItems[k].BlockID = lastID
		blockItems[k].BlockHeight = record.BlockHeight
		if returnBlock {
			blockItems[k].Block.Value = &record.Block
		}
		if returnReceipt {
			// TODO: Internally, Block Receipt needs to change
			// blockItems[k].BlockReceipt = record.BlockReceipt
		}

		if len(record.PreviousBlockIds) < 1 {
			if i+1 < numBlocks {
				return nil, &TraverseBeforeGenesisError{}
			}
		} else {
			lastID = record.PreviousBlockIds[0]
		}
	}

	return blockItems, nil
}

// GetBlocksByHeight retuns blocks by block height
func (handler *RequestHandler) GetBlocksByHeight(req *types.GetBlocksByHeightRequest) (*types.GetBlocksByHeightResponse, error) {

	resp := types.NewGetBlocksByHeightResponse()

	if req.NumBlocks <= 0 {
		return resp, nil
	}
	if req.ReturnReceipt {
		return nil, &NotImplemented{}
	}

	//resp.BlockItems = types.VectorBlockItem(make([]types.BlockItem, req.NumBlocks))

	headBlockHeight, err := getBlockHeight(handler.Backend, &req.HeadBlockID)
	if err != nil {
		return nil, err
	}

	if req.AncestorStartHeight > headBlockHeight {
		return nil, &BlockHeightMismatch{}
	}

	numBlocks := req.NumBlocks
	endHeight := uint64(req.AncestorStartHeight) + uint64(numBlocks-1)
	if endHeight > uint64(headBlockHeight) {
		endHeight = uint64(headBlockHeight)
		numBlocks = types.UInt32(endHeight - uint64(req.AncestorStartHeight) + 1)
	}

	blockID, err := getAncestorIDAtHeight(handler.Backend, &req.HeadBlockID, types.BlockHeightType(endHeight))
	if err != nil {
		if _, ok := err.(*BlockHeightMismatch); !ok {
			return nil, err
		}
	}

	resp.BlockItems, err = handler.fillBlocks(*blockID, numBlocks, req.ReturnBlock, req.ReturnReceipt)
	if err != nil {
		return nil, err
	}

	if len(resp.BlockItems) > 0 {
		expectedHeight := req.AncestorStartHeight
		if resp.BlockItems[0].BlockHeight != expectedHeight {
			log.Warnf("start  height: %d", resp.BlockItems[0].BlockHeight)
			log.Warnf("expect height: %d", expectedHeight)
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
		return 0, &BlockNotPresent{*blockID}
	}

	consumed, record, err := types.DeserializeBlockRecord((*types.VariableBlob)(&recordBytes))
	if err != nil {
		log.Warn("Couldn't deserialize block record")
		log.Warnf("vb: %v", recordBytes)
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
			return nil, &BlockNotPresent{*blockID}
		}

		consumed, record, err := types.DeserializeBlockRecord((*types.VariableBlob)(&recordBytes))
		if err != nil {
			log.Warn("Couldn't deserialize block record")
			log.Warnf("vb: %v", recordBytes)
			return nil, err
		}
		if consumed != uint64(len(recordBytes)) {
			return nil, &DeserializeError{}
		}
		if hasExpectedHeight && (record.BlockHeight != expectedHeight) {
			log.Warnf("record height: %d", record.BlockHeight)
			log.Warnf("expect height: %d", expectedHeight)
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

// AddBlock adds a block to the block store
func (handler *RequestHandler) AddBlock(req *types.AddBlockRequest) (*types.AddBlockResponse, error) {

	if !req.BlockToAdd.Block.HasValue() {
		return nil, errors.New("Cannot add empty optional block")
	}

	block := req.BlockToAdd.Block.Value
	record := types.BlockRecord{}

	record.BlockID = block.ID
	record.BlockHeight = block.Header.Height
	record.Block = *block
	// TODO: Internally, Block Receipt needs to change
	// record.BlockReceipt = req.BlockToAdd.BlockReceipt
	record.BlockReceipt = *types.NewOpaqueBlockReceipt()

	if block.Header.Height > 1 {
		previousHeights := getPreviousHeights(uint64(block.Header.Height))

		record.PreviousBlockIds = make([]types.Multihash, len(previousHeights))

		for i := 0; i < len(previousHeights); i++ {
			h := previousHeights[i]
			if h >= uint64(record.BlockHeight) {
				return nil, &InternalError{}
			} else if h == uint64(record.BlockHeight)-1 {
				record.PreviousBlockIds[i] = block.Header.Previous
			} else {
				previousID, err := getAncestorIDAtHeight(handler.Backend, &block.Header.Previous, types.BlockHeightType(h))
				if err != nil {
					return nil, err
				}
				record.PreviousBlockIds[i] = *previousID
			}
		}
	} else {
		record.PreviousBlockIds = make([]types.Multihash, 1)
		record.PreviousBlockIds[0] = block.Header.Previous
	}

	vbKey := record.BlockID.Serialize(types.NewVariableBlob())
	vbValue := record.Serialize(types.NewVariableBlob())

	err := handler.Backend.Put(*vbKey, *vbValue)
	if err != nil {
		return nil, err
	}

	resp := types.AddBlockResponse{}
	return &resp, nil
}

// GetHighestBlock returns the highest block seen by the block store
func (handler *RequestHandler) GetHighestBlock(req *types.GetHighestBlockRequest) (*types.GetHighestBlockResponse, error) {
	recordBytes, err := handler.Backend.Get(types.VariableBlob{highestBlockKey})

	if err != nil {
		return nil, err
	}

	if len(recordBytes) == 0 {
		return nil, &UnexpectedHeightError{}
	}

	valueBlob := types.VariableBlob(recordBytes)
	_, value, err := types.DeserializeBlockTopology(&valueBlob)
	if err != nil {
		log.Warn("Could not deserialize block topology")
	}

	response := types.NewGetHighestBlockResponse()
	response.Topology = *value
	return response, nil
}

// UpdateHighestBlock Updates the database metadata with the highest blocks ID
func (handler *RequestHandler) UpdateHighestBlock(topology *types.BlockTopology) error {
	recordBytes, err := handler.Backend.Get(types.VariableBlob{highestBlockKey})
	if err == nil && len(recordBytes) > 0 {
		valueBlob := types.VariableBlob(recordBytes)
		_, currentValue, err := types.DeserializeBlockTopology(&valueBlob)
		if err != nil {
			log.Warn("Could not deserialize highest block")
			return errors.New("Current highest block corrupted")
		}

		// If our current highest block height is greater, do nothing
		if currentValue.Height >= topology.Height {
			return nil
		}
	}

	newValue := topology.Serialize(types.NewVariableBlob())

	return handler.Backend.Put(types.VariableBlob{highestBlockKey}, *newValue)
}

// HandleRequest handles and routes blockstore requests
func (handler *RequestHandler) HandleRequest(req *types.BlockStoreRequest) *types.BlockStoreResponse {
	var response types.BlockStoreResponse
	var err error
	switch v := req.Value.(type) {
	case *types.BlockStoreReservedRequest:
		err = &ReservedReqError{}
		break
	case *types.GetBlocksByIDRequest:
		var result *types.GetBlocksByIDResponse
		result, err = handler.GetBlocksByID(v)
		if err == nil {
			response.Value = result
		}
		break
	case *types.GetBlocksByHeightRequest:
		var result *types.GetBlocksByHeightResponse
		result, err = handler.GetBlocksByHeight(v)
		if err == nil {
			response.Value = result
		}
		break
	case *types.AddBlockRequest:
		var result *types.AddBlockResponse
		result, err = handler.AddBlock(v)
		if err == nil {
			response.Value = result
		}
		break
	case *types.GetHighestBlockRequest:
		var result *types.GetHighestBlockResponse
		result, err = handler.GetHighestBlock(v)
		if err == nil {
			response.Value = result
		}
		break
	default:
		err = errors.New("Unknown request")
	}

	if err != nil {
		response.Value = &types.BlockStoreErrorResponse{
			ErrorText: types.String(err.Error()),
		}
	}

	return &response
}
