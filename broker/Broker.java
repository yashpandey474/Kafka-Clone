import java.util.Map;

// Broker is a kafka server, we start with only one server containing all topics
public class Broker {
    Map<String, Topic> topics;

    public void publishMessage(String message, String topicName) {
        // Given a message and topic, publish it
        Topic t = topics.get(topicName);
        t.publishMessage(message);
    }
}
