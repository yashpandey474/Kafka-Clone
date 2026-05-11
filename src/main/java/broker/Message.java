package KafkaClone.src.main.java.broker;

// Offset is needed for a message so that consumers can track until where they have read from a partition
public class Message {
    String content;
    int offset; // so that consumer also knows until which offset they had read and can continue from there

    public Message(String content, int offset) {
        this.content = content;
        this.offset = offset;
    }
}