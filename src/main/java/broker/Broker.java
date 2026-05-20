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

    public boolean createTopic(String topicName, int numPartitions) {
        if (topics.containsKey(topicName)) {
            System.out.printf("The topic %s is already present\n", topicName);
            return false;
        }
        topics.put(topicName, new Topic(topicName, numPartitions));
        System.out.printf("New topic created with name %s and partitions %d\n", topicName, numPartitions);
        return true;
    }

    public boolean publishMessage(String message, String topicName) {
        // Given a message and topic, publish it
        if (!topics.containsKey(topicName)) {
            System.out.printf("Topic with name %s does not exist and auto creation set to %b \n", this.autoTopicCreate);
            if (this.autoTopicCreate == false) {
                System.out.printf("ERROR: Topic does not exist \n");
                return false;
            }
            createTopic(topicName, this.defaultPartition);
        }

        Topic t = topics.get(topicName);
        t.addMessageToTopic(message);
        System.out.printf("Published message %s to topic %s", message, topicName);
        return true;
    }

    public int getPartitions(String topicName) {
        if (!topics.containsKey(topicName)) {
            System.out.printf("Topic with name %s \n", topicName);
            return 0;
        }
        
        System.out.printf("Topic with name %s has partitions %d \n", topicName,
                topics.get(topicName).partitions.size());
                
        return topics.get(topicName).partitions.size();
    }
}
