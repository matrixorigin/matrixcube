package client

// Batch is used to collect a collection of `Request`s for a single operation,
// and the methods beginning with `Write` and `Read` create the corresponding
// `Request`s in memory
type Batch interface {
	// Write transactional write operations. Specific write operation types and
	// data in the payload, the transaction framework is responsible for the flow
	// of distributed transactions.
	// The key is data unique Key, the framework can be based on this field to
	// route to the correct Shard.
	// The payload contains the type of write operation, the operation data.
	Write(shardGroupID uint64, key []byte, payload Payload) Batch

	// WriteToShard similar to `Write`, the difference is that the Shard is specified.
	WriteToShard(shardID uint64, payload Payload) Batch

	Read(shardGroupID uint64, key []byte, payload Payload) Batch
}
