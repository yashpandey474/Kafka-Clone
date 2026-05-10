package KafkaClone.src.main.java.broker.consumer;

import java.util.Map;

public class Consumer {
    // Consumer reads from a partition and maintains offset too
    // Starting with single consumer so it reads from multiple partitions
    Map<Integer, Integer> offsets;

    // Topic that it is assigned to, multiple consumers can read from same topic
    String topic;


}
