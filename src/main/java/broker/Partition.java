package KafkaClone.src.main.java.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    // Fetch messages from a particular offset => correct kafka design to reduce latency and increase throughput
    public List<Message> getMessagesFromOffset(int offset) {
        return messages.stream()
            .skip(offset)
            .collect(Collectors.toList());
    }

    public void createAndAddMessage(String message) {
        Message m = new Message(message, messages.size());
        addMessage(m);
    }
}