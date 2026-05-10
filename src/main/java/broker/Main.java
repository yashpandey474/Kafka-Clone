package KafkaClone.src.main.java.broker;
public class Main {

    public static void main(String[] args) {
        System.out.println("hello world");
        bool automaticTopicCreation = false;

        broker.createTopic("orders", 3);
        broker.publishMessage("hello", "orders");

        broker.publishMessage("hello1", "aldnasnd");
    }
    
}
