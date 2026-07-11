package KafkaClone.src.main.java.kafkaclone.storage;
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
    OffsetIndex offsetIndex;
 

    public LogSegment(
        int baseOffset,
        int messageLimit,
        String partitionDirectoryName,
        int segmentNo
    ) {

        this.messageLimit = messageLimit;
        this.segmentNo = segmentNo;
        this.partitionDirectoryName = partitionDirectoryName;
        this.fileName = partitionDirectoryName + "/segment-" + segmentNo + ".log";
        this.logFile = new File(fileName);
        this.offsetIndex = new OffsetIndex(partitionDirectoryName, segmentNo);
        this.baseOffset = baseOffset;
        this.recover();
    }

    // separate out since recovery can be complex later on, returns true if recovery was successful
    public boolean recover() {
        // Get current offset from index file, largest offset in index + 1
        if (offsetIndex.largestRecoveredOffset == -1) {
            this.currentOffset = baseOffset;
        } else {
            // set to largest offset in index + 1 
            // later, we should check for partial writes since index might not be ideal gt
            // log is actual source of truth
            this.currentOffset = offsetIndex.largestRecoveredOffset + 1;
        }
        return true;
    }

    public int getBaseOffset() {
        return this.baseOffset;
    }

    public boolean isFull() {
        return this.messageLimit == (this.currentOffset - this.baseOffset);
    }

    public int getCurrentOffset() {
        return this.currentOffset;
    }

    public void setCurrentOffset(int currentOffset) {
        this.currentOffset = currentOffset;
    }

    public int getSegmentNo() {
        return this.segmentNo;
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

        // Add entry to index
        offsetIndex.addEntry(currentOffset, currentFileLength);

        System.out.println("After writing: " + logFile.length() + " bytes");
        setCurrentOffset(currentOffset + 1);

        return true;
    }

    public List<Message> readFromOffset(int offset) {
        // Byte number where the message for offset starts
        List<Message> result = new ArrayList<>();

        // Check valid range
        if (offset < baseOffset|| offset >= currentOffset) {
            System.out.printf("Offset requested: %d is out of range: %d - %d", offset, 0, currentOffset - 1);
            return result;
        }
        
        // Byte to read from
        long offsetByte = offsetIndex.lookupOffset(offset);
        if (offsetByte < 0) {
            System.out.printf("Offset: %d does not exist in index, cannot read any messages", offset);
            return result;
        }
        
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "r")) {
            raf.seek(offsetByte);
            
            while (raf.getFilePointer() < raf.length()) {

                Message message = MessageSerializer.deserialize(raf);

                if (message == null) {
                    break;
                }

                result.add(message);
            }
        } catch (IOException e) {
            System.out.printf(
                    "Encountered error while reading from file %s from offset %d. Error: %s%n",
                    logFile.getName(),
                    offset,
                    e.getMessage());
        }
        return result;
    }
    
}
