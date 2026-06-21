package KafkaClone.src.main.java.broker;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

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

    public LogSegment(
        int baseOffset,
        int messageLimit,
        String fileName,
        File logFile
    ) {
        this.baseOffset = baseOffset;
        this.currentOffset = baseOffset;
        this.messageLimit = messageLimit;
        this.fileName = fileName;
        this.logFile = logFile;

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

        // Now, we are dealing with message bytes and not strings
        try (FileOutputStream fos = new FileOutputStream(logFile)) {
            fos.write(messageBytes);
        } catch (IOException e) {
            System.out.printf("Encountered error while writing %s : %s at offset %d to file %s. Error: %s", key, value,
                    currentOffset, logFile, e.getMessage());
            return false;
        }

        setCurrentOffset(currentOffset + 1);
        return true;
    }
    
    public List<Message> readFromOffset(int offset) {
        List<Message> result = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(logFile)) {
            BufferedInputStream bis = new BufferedInputStream(fis);
            
            // Skip lines before offset //TODO: Add an index file
            int linesSkipped = 0;
            int b;
            while (linesSkipped < offset && (b = bis.read()) != -1) {
                if (b == '\n') {
                    linesSkipped++;
                }
            }   
          

            // Read the file, reading each line as a list of bytes
            ByteArrayOutputStream currentBytes = new ByteArrayOutputStream();
            while ((b = bis.read()) != -1) {
                if (b == '\n') {
                    // Parse the current line of bytes
                    result.add(MessageSerializer.deSerialize(currentBytes.toByteArray()));
                    currentBytes.reset();
                } else {
                    currentBytes.write(b);
                }
            }
            
        } catch (IOException e) {
            System.out.printf("Encountered error while reading from file %s from offset: %d. Error: %s",
                    currentOffset, logFile, e.getMessage());
        }
        return result;
    }
    
}
