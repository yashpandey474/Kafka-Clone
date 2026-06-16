package KafkaClone.src.main.java.broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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

    public boolean writeMessage(Message message) throws IOException {
        // Open the file and write the message at the offset or keep the file open?
        // open file in append mode
        FileWriter writer = null;
        try {
            writer = new FileWriter(logFile, true);
        } catch (Exception e) {
            if (writer != null) {
                writer.close();
            }
            System.out.printf("Encountered an error while writing message to file %s: %s", fileName, e.getMessage());
            return false;
        }
        writer.write(currentOffset + ":" + message.content + "\n");
        writer.close();
        setCurrentOffset(currentOffset + 1);
        return true;
    }
    
    public List<Message> readFromOffset(int offset) {
        List<Message> result = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;

            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":");
                int msgOffset = Integer.parseInt(parts[0]);
                if (msgOffset >= offset) {
                    result.add(new Message(parts[1], msgOffset));
                }
            }
        } catch (Exception e) {
            System.out.printf("Encountered error while reading from file %s: %s", fileName, e.getMessage());
        }

        return result;
    }
    
}
