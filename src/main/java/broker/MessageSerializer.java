package KafkaClone.src.main.java.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class MessageSerializer {
    // This class handles serialising the message to bytes and deserialising back to message
    public static byte[] serialise(Message message) {

        // Convert string to bytes
        byte[] keyBytes = message.key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = message.value.getBytes(StandardCharsets.UTF_8);

        // Allocate bytes according to contract
        int offsetBytes = 8;
        int timestampBytes = 8;
        int keyLengthBytes = 4;
        int keyValueBytes = keyBytes.length;
        int valueFieldLengthBytes = 4;
        int valueFieldValueBytes = valueBytes.length;

    
        ByteBuffer buffer = ByteBuffer.allocate(offsetBytes + timestampBytes + keyLengthBytes + keyValueBytes
                + valueFieldLengthBytes + valueFieldValueBytes);
        
        // Put data into the buffer
        buffer.putInt(message.offset);
        buffer.putLong(message.timestamp.getEpochSecond());
        buffer.putInt(keyValueBytes);
        buffer.put(keyBytes);
        buffer.putInt(valueFieldValueBytes);
        buffer.put(valueBytes);

        return buffer.array();
    }

    public static Message deSerialize(byte[] byteArray) {
        // Create bytebuffer
        ByteBuffer buffer = ByteBuffer.wrap(byteArray);

        int offset = buffer.getInt();
        
        long seconds = buffer.getLong();
        Instant timestamp = Instant.ofEpochSecond(seconds);

        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);

        String messageKey = new String(keyBytes, StandardCharsets.UTF_8);

        int valueLength = buffer.getInt();
        byte[] valueBytes = new byte[valueLength];
        buffer.get(valueBytes);

        String messageValue = new String(valueBytes, StandardCharsets.UTF_8);

        return new Message(messageKey, messageValue, offset, timestamp);
    }
}
