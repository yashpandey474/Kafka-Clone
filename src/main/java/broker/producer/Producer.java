package KafkaClone.src.main.java.broker.producer;
import KafkaClone.src.main.java.broker.Broker;

public class Producer {
    // Producer publishes messages to a broker
    Broker broker;

    public Producer() {
        this.broker = new Broker();
    }
}
