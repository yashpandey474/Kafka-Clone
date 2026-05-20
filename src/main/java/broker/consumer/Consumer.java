package KafkaClone.src.main.java.broker.consumer;

import java.util.Map;

import KafkaClone.src.main.java.broker.Broker;

public class Consumer {
    // Consumer reads from a partition and maintains offset too
    // Starting with single consumer so it reads from multiple partitions
    Map<Integer, Integer> offsets;

    // Topic that it is assigned to, multiple consumers can read from same topic
    String topic;

    // Broker that provides it with messages for the partitions, later could be a wrapper with list of Brokers
    Broker broker;

    // Should know topic and broker
    public Consumer(String topic, Broker broker) {
        this.broker = broker;
        this.topic = topic;
        initialiseOffsets();
    }

    public void initialiseOffsets() {
        
    }

}
