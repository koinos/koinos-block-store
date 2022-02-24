package bstore

import (
	"errors"
	"fmt"
	"math/bits"

	base58 "github.com/btcsuite/btcutil/base58"
	log "github.com/koinos/koinos-log-golang"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/koinos/koinos-proto-golang/koinos/rpc"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	"google.golang.org/protobuf/proto"
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
	blockID []byte
}

func (e *BlockNotPresent) Error() string {
	return fmt.Sprintf("Block not present - ID: %v", base58.Encode(e.blockID))
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
func (handler *RequestHandler) GetBlocksByID(req *block_store.GetBlocksByIdRequest) (*block_store.GetBlocksByIdResponse, error) {
	result := block_store.GetBlocksByIdResponse{}

	result.BlockItems = make([]*block_store.BlockItem, len(req.BlockIds))

	if req.BlockIds == nil {
		return nil, errors.New("expected field 'block_id' was nil")
	}

	for i := range req.GetBlockIds() {
		if req.GetBlockIds()[i] == nil {
			return nil, errors.New("member of field 'block_id' was nil")
		}

		bytes, err := handler.Backend.Get(req.GetBlockIds()[i])
		if err != nil {
			continue
		}

		record := block_store.BlockRecord{}
		err = proto.Unmarshal(bytes, &record)
		if err != nil {
			continue
		}

		result.BlockItems[i] = &block_store.BlockItem{BlockId: record.GetBlockId(), BlockHeight: record.GetBlockHeight()}

		if req.GetReturnBlock() {
			result.BlockItems[i].Block = record.Block
		}

		if req.GetReturnReceipt() {
			result.BlockItems[i].Receipt = record.Receipt
		}
	}

	return &result, nil
}

/**
 * Internal helper method to fill blocks.
 *
 * Given a block ID and height, return the block and the previous numBlocks-1 blocks.
 * Return empty block if we go past the beginning.
 */
func (handler *RequestHandler) fillBlocks(
	lastID []byte,
	numBlocks uint32,
	returnBlock bool,
	returnReceipt bool) ([]*block_store.BlockItem, error) {
	blockItems := make([]*block_store.BlockItem, numBlocks)

	if numBlocks <= 0 {
		return blockItems, nil
	}

	//blockID := *lastID

	var i uint32
	for i = 0; i < numBlocks; i++ {
		// k is the index into the array
		k := numBlocks - i - 1

		recordBytes, err := handler.Backend.Get(lastID)
		if err != nil {
			return nil, err
		}
		if len(recordBytes) == 0 {
			// If block does not exist, return a default-initialized block.
			continue
		}

		record := block_store.BlockRecord{}
		err = proto.Unmarshal(recordBytes, &record)
		if err != nil {
			log.Warn("Couldn't deserialize block record")
			log.Warnf("vb: %v", recordBytes)
			return nil, err
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

		blockItems[k] = &block_store.BlockItem{BlockId: lastID, BlockHeight: record.BlockHeight}
		if returnBlock {
			blockItems[k].Block = record.Block
		}
		if returnReceipt {
			blockItems[k].Receipt = record.Receipt
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
func (handler *RequestHandler) GetBlocksByHeight(req *block_store.GetBlocksByHeightRequest) (*block_store.GetBlocksByHeightResponse, error) {

	resp := block_store.GetBlocksByHeightResponse{}

	if req.NumBlocks <= 0 {
		return &resp, nil
	}

	if req.AncestorStartHeight == 0 {
		return nil, errors.New("ancestor_start_height must be greater than 0")
	}

	if req.HeadBlockId == nil {
		return nil, errors.New("expected field, 'head_block_id' was nil")
	}

	headBlockHeight, err := getBlockHeight(handler.Backend, req.HeadBlockId)
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
		numBlocks = uint32(endHeight - uint64(req.AncestorStartHeight) + 1)
	}

	blockID, err := getAncestorIDAtHeight(handler.Backend, req.HeadBlockId, endHeight)
	if err != nil {
		if _, ok := err.(*BlockHeightMismatch); !ok {
			return nil, err
		}
	}

	resp.BlockItems, err = handler.fillBlocks(blockID, numBlocks, req.GetReturnBlock(), req.ReturnReceipt)
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

	return &resp, nil
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
func getPreviousHeightIndex(goal uint64, current uint64) (int, uint64, error) {
	if goal >= current {
		return 0, 0, &BlockHeightMismatch{}
	}

	var x uint64 = current
	var g uint64 = goal
	zeros := bits.TrailingZeros64(x)

	var lastH uint64 = 0
	for i := 0; i <= zeros; i++ {
		h := x - (uint64(1) << i)
		if h < g {
			return i - 1, lastH, nil
		}
		lastH = h
	}
	return zeros, lastH, nil
}

/**
 * Fetch a block by ID and then return its height.
 */
func getBlockHeight(backend BlockStoreBackend, blockID []byte) (uint64, error) {
	recordBytes, err := backend.Get(blockID)
	if err != nil {
		return 0, err
	}
	if len(recordBytes) == 0 {
		return 0, &BlockNotPresent{blockID}
	}

	record := block_store.BlockRecord{}
	err = proto.Unmarshal(recordBytes, &record)
	if err != nil {
		log.Warn("Couldn't deserialize block record")
		log.Warnf("vb: %v", recordBytes)
		return 0, err
	}

	return record.BlockHeight, nil
}

func getAncestorIDAtHeight(backend BlockStoreBackend, blockID []byte, height uint64) ([]byte, error) {

	var expectedHeight uint64
	var hasExpectedHeight bool = false

	for {
		recordBytes, err := backend.Get(blockID)
		if err != nil {
			return nil, err
		}
		if len(recordBytes) == 0 {
			return nil, &BlockNotPresent{blockID}
		}

		record := block_store.BlockRecord{}
		err = proto.Unmarshal(recordBytes, &record)
		if err != nil {
			log.Warn("Couldn't deserialize block record")
			log.Warnf("vb: %v", recordBytes)
			return nil, err
		}
		if hasExpectedHeight && (record.GetBlockHeight() != expectedHeight) {
			log.Warnf("record height: %d", record.GetBlockHeight())
			log.Warnf("expect height: %d", expectedHeight)
			return nil, &UnexpectedHeightError{}
		}

		if record.GetBlockHeight() == height {
			return record.GetBlockId(), nil
		}

		newIndex, newHeight, err := getPreviousHeightIndex(height, record.GetBlockHeight())
		if err != nil {
			return nil, err
		}
		if newIndex >= len(record.PreviousBlockIds) {
			return nil, &UnexpectedHeightError{}
		}

		// We only care about the ID, so once we've found it in a previous list, no need to actually fetch the record
		blockID = record.GetPreviousBlockIds()[newIndex]
		if newHeight == height {
			return blockID, nil
		}
		expectedHeight = newHeight
		hasExpectedHeight = true
	}
}

// AddBlock adds a block to the block store
func (handler *RequestHandler) AddBlock(req *block_store.AddBlockRequest) (*block_store.AddBlockResponse, error) {

	if req.GetBlockToAdd() == nil {
		return nil, errors.New("Cannot add empty optional block")
	}

	block := req.GetBlockToAdd()
	record := block_store.BlockRecord{}

	record.BlockId = block.GetId()
	record.BlockHeight = block.GetHeader().GetHeight()
	record.Block = block

	record.Receipt = req.GetReceiptToAdd()

	if block.Header.Height > 1 {
		previousHeights := getPreviousHeights(block.GetHeader().GetHeight())

		record.PreviousBlockIds = make([][]byte, len(previousHeights))

		for i := 0; i < len(previousHeights); i++ {
			h := previousHeights[i]
			if h >= record.GetBlockHeight() {
				return nil, &InternalError{}
			} else if h == uint64(record.BlockHeight)-1 {
				record.PreviousBlockIds[i] = block.GetHeader().GetPrevious()
			} else {
				previousID, err := getAncestorIDAtHeight(handler.Backend, block.GetHeader().GetPrevious(), h)
				if err != nil {
					return nil, err
				}
				record.PreviousBlockIds[i] = previousID
			}
		}
	} else {
		record.PreviousBlockIds = make([][]byte, 1)
		record.PreviousBlockIds[0] = block.Header.Previous
	}

	vbValue, err := proto.Marshal(&record)
	if err != nil {
		return nil, err
	}

	err = handler.Backend.Put(record.GetBlockId(), vbValue)
	if err != nil {
		return nil, err
	}

	resp := block_store.AddBlockResponse{}
	return &resp, nil
}

// GetHighestBlock returns the highest block seen by the block store
func (handler *RequestHandler) GetHighestBlock(req *block_store.GetHighestBlockRequest) (*block_store.GetHighestBlockResponse, error) {
	recordBytes, err := handler.Backend.Get([]byte{highestBlockKey})

	if err != nil {
		return nil, err
	}

	if len(recordBytes) == 0 {
		return nil, &UnexpectedHeightError{}
	}

	value := koinos.BlockTopology{}
	err = proto.Unmarshal(recordBytes, &value)
	if err != nil {
		log.Warn("Could not deserialize block topology")
	}

	response := block_store.GetHighestBlockResponse{}
	response.Topology = &value
	return &response, nil
}

// UpdateHighestBlock Updates the database metadata with the highest blocks ID
func (handler *RequestHandler) UpdateHighestBlock(topology *koinos.BlockTopology) error {
	recordBytes, err := handler.Backend.Get([]byte{highestBlockKey})
	if err == nil && len(recordBytes) > 0 {
		currentValue := koinos.BlockTopology{}
		err = proto.Unmarshal(recordBytes, &currentValue)
		if err != nil {
			log.Warn("Could not deserialize highest block")
			return errors.New("Current highest block corrupted")
		}

		// If our current highest block height is greater, do nothing
		if currentValue.GetHeight() >= topology.GetHeight() {
			return nil
		}
	}

	newValue, err := proto.Marshal(topology)
	if err != nil {
		return err
	}

	return handler.Backend.Put([]byte{highestBlockKey}, newValue)
}

// HandleRequest handles and routes blockstore requests
func (handler *RequestHandler) HandleRequest(req *block_store.BlockStoreRequest) *block_store.BlockStoreResponse {
	response := block_store.BlockStoreResponse{}
	var err error

	if req.Request != nil {
		switch v := req.Request.(type) {
		case *block_store.BlockStoreRequest_GetBlocksById:
			var result *block_store.GetBlocksByIdResponse
			result, err = handler.GetBlocksByID(v.GetBlocksById)
			if err == nil {
				respVal := block_store.BlockStoreResponse_GetBlocksById{GetBlocksById: result}
				response.Response = &respVal
			}
			break
		case *block_store.BlockStoreRequest_GetBlocksByHeight:
			var result *block_store.GetBlocksByHeightResponse
			result, err = handler.GetBlocksByHeight(v.GetBlocksByHeight)
			if err == nil {
				respVal := block_store.BlockStoreResponse_GetBlocksByHeight{GetBlocksByHeight: result}
				response.Response = &respVal
			}
			break
		case *block_store.BlockStoreRequest_AddBlock:
			var result *block_store.AddBlockResponse
			result, err = handler.AddBlock(v.AddBlock)
			if err == nil {
				respVal := block_store.BlockStoreResponse_AddBlock{AddBlock: result}
				response.Response = &respVal
			}
			break
		case *block_store.BlockStoreRequest_GetHighestBlock:
			var result *block_store.GetHighestBlockResponse
			result, err = handler.GetHighestBlock(v.GetHighestBlock)
			if err == nil {
				respVal := block_store.BlockStoreResponse_GetHighestBlock{GetHighestBlock: result}
				response.Response = &respVal
			}
			break
		default:
			err = errors.New("Unknown request")
		}
	} else {
		err = errors.New("expected request was nil")
	}

	if err != nil {
		result := rpc.ErrorResponse{Message: err.Error()}
		respVal := block_store.BlockStoreResponse_Error{Error: &result}
		response.Response = &respVal
	}

	return &response
}
