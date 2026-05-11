package KafkaClone.src.main.java.broker;
public class Main {

    public static void main(String[] args) {
        boolean automaticTopicCreation = false;
        int defaultPartition = 3;
        

        broker.createTopic("orders", 3);
        broker.publishMessage("hello", "orders");

        broker.publishMessage("hello1", "aldnasnd");
    }
    
}
