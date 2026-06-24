package KafkaClone.src.main.java.broker;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// What is a log segment?
// A partition needs to persist the messages it stores to achieve persistence in case of failure
// Instead of storing all messages into one file, it stores messages in segments
// A segment may store 1000 messages (or uptill 100MB file size)
// Segment abstracts away writing to the file
// Until it reaches a number of messages, in which case a new segment is created
public class LogSegment {
    File logFile;
    String fileName;
    String topicName;
    int partitionNo;
    int baseOffset;
    int currentOffset;
    int messageLimit;
    String partitionDirectoryName;
    int segmentNo;

    // Index: Store offset to byte
    File indexFile;
    Map<Integer, Long> offsetIndex; // Offset -> byte number

    public LogSegment(
        int baseOffset,
        int messageLimit,
        String partitionDirectoryName,
        int segmentNo
    ) {
        this.baseOffset = baseOffset;
        this.currentOffset = baseOffset;
        this.messageLimit = messageLimit;
        this.partitionDirectoryName = partitionDirectoryName;
        this.segmentNo = segmentNo;
        this.fileName = partitionDirectoryName + "/segment-" + segmentNo + ".log";
        this.logFile = new File(fileName);
        this.indexFile = new File(partitionDirectoryName + "/segment-" + segmentNo + ".index");
    }

    public boolean isFull() {
        return this.messageLimit == (this.currentOffset - this.baseOffset);
    }

    public void setCurrentOffset(int currentOffset) {
        this.currentOffset = currentOffset;
    }

    public boolean writeMessage(String key, String value) throws IOException {
        // Create message
        Message message = new Message(key, value, currentOffset);

        // Serialise message to bytes
        byte[] messageBytes = MessageSerializer.serialise(message);
        int messageLength = messageBytes.length;
        ByteBuffer record = ByteBuffer.allocate(4 + messageLength);
        record.putInt(messageLength);
        record.put(messageBytes);

        // Now, we are dealing with message bytes and not strings
        long currentFileLength = logFile.length(); // Number of bytes in file currently
        System.out.println("Before write: " + logFile.length() + " bytes\nMessage size: " + messageBytes.length);

        try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
            fos.write(record.array());
        } catch (IOException e) {
            System.out.printf("Encountered error while writing %s : %s at offset %d to file %s. Error: %s", key, value,
                    currentOffset, logFile, e.getMessage());
            return false;
        }

        // Store offset -> byte, message of this offset starts at this byte
        offsetIndex.put(currentOffset, currentFileLength);

        // Write to the index file
        String indexEntry = currentOffset + "," + currentFileLength + "\n"; // Make this binary later, maybe 2 functions
                                                                            // in MessageSerializer
        try (FileWriter fw = new FileWriter(indexFile, true)) { // use random access file
            fw.write(indexEntry);
        }

        System.out.println("After writing: " + logFile.length() + " bytes");
        setCurrentOffset(currentOffset + 1);

        return true;
    }

    public List<Message> readFromOffset(int offset) {
        // Byte number where the message for offset starts
        List<Message> result = new ArrayList<>();
        
        // Byte to read from
        if (offsetIndex.get(offset) == null) {
            System.out.printf("Offset: %d does not exist in index, cannot read any messages", offset);
            return result;
        }

        long offsetByte = offsetIndex.get(offset);
        
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "r")) {
            raf.seek(offsetByte);
            
            
        } catch (IOException e) {
            System.out.printf("Encountered error while reading from file %s from offset: %d. Error: %s",
                    currentOffset, logFile, e.getMessage());
        }
        return result;
    }
    
}
