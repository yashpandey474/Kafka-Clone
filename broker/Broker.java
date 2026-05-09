import java.util.Map;

// Broker is a kafka server, we start with only one server containing all topics
public class Broker {
    Map<String, Topic> topics;
}
