package KafkaClone.src.main.java.broker.producer;
import KafkaClone.src.main.java.broker.Broker;

public class Producer {
    // Producer publishes messages to a broker
    Broker broker;

    public Producer(boolean autoTopicCreate, int defaultPartition) {
        this.broker = new Broker(autoTopicCreate,defaultPartition);
    }
}
