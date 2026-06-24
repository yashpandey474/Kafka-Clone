package KafkaClone.src.main.java.broker.producer;
import KafkaClone.src.main.java.broker.Broker;

public class Producer {
    // Producer publishes messages to a broker
    Broker broker;

    public Producer(Broker broker) {
        this.broker = broker;
    }
    public boolean createTopic(String topicName, int numPartitions, int messageLimitPerSegment) {
        return this.broker.createTopic(topicName, numPartitions, messageLimitPerSegment);
    }
    public boolean publishMessage(String key, String value, String topicName) {
        boolean val = this.broker.publishMessage(key, value, topicName);
        System.out.printf("Message %s : %s published to topic %s", key, value, topicName);
        return val;
    }
}
