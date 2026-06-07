package KafkaClone.src.main.java.broker;

import KafkaClone.src.main.java.broker.producer.Producer;

public class Main {

    public static void main(String[] args) {
        // 
        boolean automaticTopicCreation = false;
        int defaultPartition = 3;
        
        // Initialise Kafka Brokers
        Broker b = new Broker(automaticTopicCreation, defaultPartition);

        //Initialie a producer - independent from Brokers
        Producer p = new Producer(b);

        // Create a topic
        p.createTopic("topic1", 3);

        // Publish a message
        p.publishMessage("message1", "topic1");

        // Consumers
        // 1 consumer subscribes to a particular topic
        // It asks the broker for number of partitions when subscribing
        // When polling, it asks for all unread messages from an offset

    }
}
