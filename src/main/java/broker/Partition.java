package KafkaClone.src.main.java.broker;

import java.util.List;

// Partition is what actually holds the messages for a particular topic
public class Partition {
    List<Message> messages;

    // Partition adds message to its actual message queue
    public void addMessage(Message message) {
        messages.add(message);
    }

    // Fetch message at a particular offset
    public Message getMessage(int offset) {
        if (offset >= messages.size() || offset < 0) {
            return null;
        }
        return messages.get(offset);
    }
}
