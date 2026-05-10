package KafkaClone.src.main.java.broker;

import java.util.ArrayList;
import java.util.List;

// A topic is a logical category of messages
// Partitions are used for scalability so that multiple producers and consumers can write and read at same time repectively
public class Topic {
    String topicName;
    List<Partition> partitions;

    // Round robin to partitions
    int currPartition;

    public Topic(String topicName, Integer numPartitions) {
        this.topicName = topicName;
        this.currPartition = 0;

        partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new Partition());
        }
    }

    public void publishMessage(String message) {
        // Publish a message to a particular partition
        // [0, 1, 2], messages: a, b, c, d, e-> [0, 1, 2]?
        partitions.get(currPartition);
        currPartition = (currPartition + 1) % partitions.size();
    }
}