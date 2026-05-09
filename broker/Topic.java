import java.util.ArrayList;
import java.util.List;

// A topic is a logical category of messages
// Partitions are used for scalability so that multiple producers and consumers can write and read at same time repectively
public class Topic {
    String topicName;
    List<Partition> partitions;

    public Topic(String topicName, Integer numPartitions) {
        partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new Partition());
        }
    }

    public void publishMessage(String message) {
        // Publish a message to a particular partition
    }
}