// Offset is needed for a message so that consumers can track until where they have read from a partition
public class Message {
    String content;
    Integer offset;
}