package KafkaClone.src.main.java.broker.consumer;
import KafkaClone.src.main.java.broker.Message;

import java.util.List;
import java.util.Map;

import KafkaClone.src.main.java.broker.Broker;

public class Consumer {
    // Consumer reads from a partition and maintains offset too
    // Starting with single consumer so it reads from multiple partitions
    Map<Integer, Integer> offsets;
    List<Message> allMessages;

    // Topic that it is assigned to, multiple consumers can read from same topic
    String topicName;

    // Broker that provides it with messages for the partitions, later could be a wrapper with list of Brokers
    Broker broker;

    // Should know topic and broker
    public Consumer(String topicName, Broker broker) {
        this.broker = broker;
        this.topicName = topicName;
        initialiseOffsets();
    }

    public void initialiseOffsets() {
        for (int i = 0; i < broker.getPartitions(topicName); i++) {
            offsets.put(i, 0);
        }
    }

    public void poll() {
        for (Map.Entry<Integer, Integer> e : offsets.entrySet()) {
            // Poll for messages from a partition and offset
            List<Message> newMessages = broker.getMessages(topicName, e.getKey(), e.getValue());

            // process the messages, add it to a list
            allMessages.addAll(newMessages);

            // Update the offset
            offsets.put(e.getKey(), e.getValue() + newMessages.size());
        }
    }
}
