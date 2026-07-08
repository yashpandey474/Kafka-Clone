## Why does kafka use key:value pair instead of simple string?
1. Guaranteeing Message Ordering
Kafka only guarantees strict message order within a specific partition, not across an entire topic.
With a key: Kafka runs a hashing algorithm (like MurmurHash2) on the key to route all messages with that same key to the exact same partition. 
For instance, using a user_id as a key ensures that all transactions for that specific user are processed sequentially in chronological order.

Without a key: Kafka distributes messages randomly or via a round-robin strategy across partitions. If you send an sequence of related strings without a key, they will end up on different partitions and can be processed out of order.

2. Log Compaction
Kafka offers a data retention policy called Log Compaction.Under this policy, Kafka periodically cleans up a topic by looking at the keys and only keeping the most recent value for any given key.

This turns a Kafka topic into a changelog or a key-value database, which is crucial for applications that need to restore application state or look up the latest snapshot of a record.