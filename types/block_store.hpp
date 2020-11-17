namespace koinos { namespace types { namespace block_store {

struct reserved_req {};

struct reserved_resp {};

struct get_blocks_by_id_req
{
   /**
    * The ID's of the blocks to get.
    */
   std::vector< types::multihash >       block_id;

   /**
    * If true, returns the blocks' contents.
    */
   boolean                               return_block_blob;

   /**
    * If true, returns the blocks' receipts.
    */
   boolean                               return_receipt_blob;
};

// TODO Is there a better name for this data structure than block_item?
struct block_item
{
   /**
    * The hash of the block.
    */
   types::multihash                      block_id;

   /**
    * The height of the block.
    */
   types::block_height_type              block_height;

   /**
    * The block data.  If return_block_blob is false, block_blob will be empty.
    */
   types::variable_blob                  block_blob;

   /**
    * The block data.  If return_receipt_blob is false, block_receipt_blob will be empty.
    */
   types::variable_blob                  block_receipt_blob;
};

struct get_blocks_by_id_resp
{
   std::vector< block_item >             block_items;
};

struct get_blocks_by_height_req
{
   types::multihash                      head_block_id;
   types::block_height_type              ancestor_start_height;
   uint32                                num_blocks;

   boolean                               return_block_blob;
   boolean                               return_receipt_blob;
};

struct get_blocks_by_height_resp
{
   std::vector< block_item >             block_items;
};

struct add_block_req
{
   block_item                            block_to_add;
   types::multihash                      previous_block_id;
};

struct add_block_resp
{
};

struct block_record
{
   types::multihash                      block_id;
   types::block_height_type              block_height;
   std::vector< types::multihash >       previous_block_ids;

   types::variable_blob                  block_blob;
   types::variable_blob                  block_receipt_blob;
};

typedef std::variant<
   reserved_req,
   get_blocks_by_id_req,
   get_blocks_by_height_req,
   add_block_req
   > block_store_req;

typedef std::variant<
   reserved_resp,
   get_blocks_by_id_resp,
   get_blocks_by_height_resp,
   add_block_resp
   > block_store_resp;

} } }
