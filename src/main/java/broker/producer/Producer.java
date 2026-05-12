package KafkaClone.src.main.java.broker.producer;
import KafkaClone.src.main.java.broker.Broker;

public class Producer {
    // Producer publishes messages to a broker
    Broker broker;

    public Producer(boolean autoTopicCreate, int defaultPartition) {
        this.broker = new Broker(autoTopicCreate, defaultPartition);
    }
    
    public boolean createTopic(String topicName, int numPartitions) {
        return this.broker.createTopic(topicName, numPartitions);
    }
    public boolean publishMessage(String message, String topicName) {
        boolean val = this.broker.publishMessage(message, topicName);
        System.out.printf("Message %s published to topic %s", message, topicName);
        return val;
    }
}
