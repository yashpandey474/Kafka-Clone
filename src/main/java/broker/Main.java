package KafkaClone.src.main.java.broker;

import KafkaClone.src.main.java.broker.producer.Producer;

public class Main {

    public static void main(String[] args) {
        boolean automaticTopicCreation = false;
        int defaultPartition = 3;
        
        //Initialie a producer
        Producer p = new Producer(automaticTopicCreation, defaultPartition);

        // Create a topic
        p.createTopic("topic1", 3);

        // Publish a message
        p.publishMessage("message1", "topic1");
    }
    
}
