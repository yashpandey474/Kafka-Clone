import java.util.List;

// A topic is a logical category of messages
// Partitions are used for scalability so that multiple producers and consumers can write and read at same time repectively
public class Topic {
    String topicName;
    List<Partition> partitions;
}