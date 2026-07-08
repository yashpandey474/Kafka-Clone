package KafkaClone.src.main.java.kafkaclone;

import KafkaClone.src.main.java.kafkaclone.broker.Broker;
import KafkaClone.src.main.java.kafkaclone.producer.Producer;

public class Main {

    public static void main(String[] args) {
        // 
        boolean automaticTopicCreation = false;
        int defaultPartition = 3;
        int defaultMessageLimitPerSegment = 1000;
        
        // Initialise Kafka Brokers
        Broker b = new Broker(automaticTopicCreation, defaultPartition, defaultMessageLimitPerSegment);

        //Initialie a producer - independent from Brokers
        Producer p = new Producer(b);

        // Create a topic
        p.createTopic("topic1", 3, 1000);

        // Publish a message
        p.publishMessage("message-key-1", "message-value-1",  "topic1");

        // Consumers
        // 1 consumer subscribes to a particular topic
        // It asks the broker for number of partitions when subscribing
        // When polling, it asks for all unread messages from an offset

    }
}
