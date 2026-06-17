package KafkaClone.src.main.java.broker;

import java.time.Instant;

// Offset is needed for a message so that consumers can track until where they have read from a partition
public class Message {
    // kafka -> lists -> 10 message -> consumer, 10 -> 11th message
    String key; // Key is used to partition/distribute data and log compaction
    String value;

    int offset; // so that consumer also knows until which offset they had read and can continue from there
    Instant timestamp;

    public Message(String key, String value, int offset) {
        this.key = key;
        this.value = value;
        this.offset = offset;
        this.timestamp = Instant.now();
    }
    
    public Message(String key, String value, int offset, Instant timestamp) {
        this.key = key;
        this.value = value;
        this.offset = offset;
        this.timestamp = timestamp;
    }
}