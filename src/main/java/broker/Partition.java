package KafkaClone.src.main.java.broker;

import java.util.ArrayList;
import java.util.List;

// Partition is what actually holds the messages for a particular topic
public class Partition {
    int partitionNo;
    List<Message> messages;

    public Partition(int partitionNo) {
        this.partitionNo = partitionNo;
        this.messages = new ArrayList<>();
    }

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

    public void createAndAddMessage(String message) {
        Message m = new Message(message, messages.size());
        addMessage(m);
        
    }
}