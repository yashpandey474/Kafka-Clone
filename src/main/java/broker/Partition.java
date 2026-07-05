package KafkaClone.src.main.java.broker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import KafkaClone.src.main.java.broker.storage.LogSegment;
import KafkaClone.src.main.java.broker.storage.Message;

// Partition is what actually holds the messages for a particular topic
public class Partition {
    int partitionNo;
    List<LogSegment> segments;
    LogSegment activeSegment;
    int messageLimitPerSegment;
    int currentOffset;
    String partitionDirectoryName;

    public Partition(int partitionNo, int messageLimitPerSegment, String topicName, String topicDirectoryName) {
        this.partitionNo = partitionNo;
        this.messageLimitPerSegment = messageLimitPerSegment;
        this.segments = new ArrayList<>(); // need to implement recovery - read from existing files based on partitionNo and a base director
        this.currentOffset = 0;

        this.partitionDirectoryName = topicDirectoryName + "/partition-" + partitionNo;
        File partitionDirectory = new File(partitionDirectoryName);
        partitionDirectory.mkdirs();
    }

    public int getCurrentOffset() {
        return this.currentOffset;
    }

    public void setCurrentOffset(int offset) {
        this.currentOffset = offset;
    }

    // Partition adds message to its actual message queue
    public void addMessage(String key, String value) throws IOException { 
        // Initialise the new segment
        if (activeSegment.isFull()) {
            this.activeSegment = new LogSegment(currentOffset, messageLimitPerSegment, partitionDirectoryName,
                    segments.size());
            segments.add(activeSegment);
        }

        activeSegment.writeMessage(key, value);
        currentOffset++;
    }

    // Fetch messages from a particular offset => correct kafka design to reduce latency and increase throughput
    public List<Message> getMessagesFromOffset(int offset) throws NumberFormatException, IOException {
        // read from file
        return activeSegment.readFromOffset(offset);
    }
}