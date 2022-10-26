package bstore

// BlockStoreBackend interface defines an abstract key-value store
type BlockStoreBackend interface {
	/**
	 * Store the given value in the given key.
	 */
	Put(key []byte, value []byte) error

	/**
	 * Deletes the value at the given key.
	 */
	Delete(key []byte) error

	/**
	 * Get a previously stored value.
	 *
	 * If the key is not found, returns (nil, nil).
	 */
	Get(key []byte) ([]byte, error)

	// Resets the entire database
	Reset() error
}
