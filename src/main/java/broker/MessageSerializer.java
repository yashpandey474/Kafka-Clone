package KafkaClone.src.main.java.broker;

import java.nio.ByteBuffer;

public class MessageSerializer {
    // This class handles serialising the message to bytes and deserialising back to message
    public byte[] serialise(Message message) {
        // Allocate bytes according to contract
        int offsetBytes = 8;
        int timestampBytes = 8;
        int keyLengthBytes = 4;
        int keyValueBytes = message.key.length();
        int valueFieldLengthBytes = 4;
        int valueFieldValueBytes = message.value.length();

        ByteBuffer buffer = ByteBuffer.allocate(offsetBytes + timestampBytes + keyLengthBytes + keyValueBytes
                + valueFieldLengthBytes + valueFieldValueBytes);
        
        // Put data into the buffer
        buffer.putInt(message.offset);
        buffer.putLong()
    }
}
