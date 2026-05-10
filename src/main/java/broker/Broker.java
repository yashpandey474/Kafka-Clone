package KafkaClone.src.main.java.broker;
import java.util.HashMap;
import java.util.Map;


// Broker is a kafka server, we start with only one server containing all topics
public class Broker {
    Map<String, Topic> topics;
    boolean autoTopicCreate;
    int defaultPartition;

    public Broker(boolean autoTopicCreate, int defaultPartition) {
        this.topics = new HashMap<>();
        this.defaultPartition = defaultPartition;
        this.autoTopicCreate = autoTopicCreate;
    }
    
    public void createTopic(String topicName, int numPartitions) {
        if (topics.containsKey(topicName)) {
            System.out.printf("The topic %s is already present\n", topicName);
        }
        topics.put(topicName, new Topic(topicName, numPartitions));
        System.out.printf("New topic created with name %s and partitions %d\n", topicName, numPartitions);
    }


    public void publishMessage(String message, String topicName) {
        // Given a message and topic, publish it
        
        if (!topics.containsKey(topicName)) {
            System.out.printf("Topic with name %s does not exist and auto creation set to", topicName);
            return;
        }
        
        Topic t = topics.get(topicName);
        t.publishMessage(message);
    }
}
