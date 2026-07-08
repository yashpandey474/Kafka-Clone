package KafkaClone.src.main.java.kafkaclone.broker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import KafkaClone.src.main.java.kafkaclone.storage.LogSegment;
import KafkaClone.src.main.java.kafkaclone.storage.Message;

// Partition is what actually holds the messages for a particular topic
public class Partition {
    int partitionNo;
    List<LogSegment> segments;
    LogSegment activeSegment;
    int messageLimitPerSegment;
    int currentOffset;
    String partitionDirectoryName;

    public Partition(int partitionNo, int messageLimitPerSegment, String topicName, String topicDirectoryName) {
        private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

        this.partitionNo = partitionNo;
        this.messageLimitPerSegment = messageLimitPerSegment;

        this.currentOffset = 0;

        this.partitionDirectoryName = topicDirectoryName + "/partition-" + partitionNo;
        File partitionDirectory = new File(partitionDirectoryName);
        partitionDirectory.mkdirs();

        // read all segment files in the directory and create logsegments
        Path path = Paths.get(partitionDirectoryName);
        List<Integer> segmentNumbers = new ArrayList<>();

        try (Stream<Path> stream = Files.list(path)) {
            stream
                    .filter(Files::isRegularFile) // Excludes directories
                    .filter(file -> file.toString().endsWith(".log"))
                    .forEach(file -> {
                        String filename = file.getFileName().toString();
                        int segmentNo = Integer.parseInt(
                            filename
                                .replace("segment-", "")
                                .replace(".log", "")
                        );
                        segmentNumbers.add(segmentNo);
                    }); 
        } catch (IOException e) {
            e.printStackTrace();
        }

        segmentNumbers.sort(null);
        LogSegment segment;
        for (int i = 0; i < segmentNumbers.size(); i++) {
            segment = new LogSegment(messageLimitPerSegment * segmentNumbers.get(i), messageLimitPerSegment, partitionDirectoryName, segmentNumbers.get(i));
            if (i == segmentNumbers.size() - 1) {
                activeSegment = segment;
            }
            segments.add(segment);
        }
    }
    
    public int getSegmentNo(int offset) {
        return (offset / messageLimitPerSegment);
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
                    segments.size() + 1);
            segments.add(activeSegment);
        }
        activeSegment.writeMessage(key, value);
        currentOffset++;
    }

    // Fetch messages from a particular offset => correct kafka design to reduce latency and increase throughput
    public List<Message> getMessagesFromOffset(int offset) throws NumberFormatException, IOException {
        // figure out correct segment
        int segNo = getSegmentNo(offset);
        if (segNo < 0 || segNo > segments.size()) {
            logger.error("Offset {} belongs to Segment {} which is out of range {} - {}", offset, segNo, 0,
                    segments.size());
            return null;
        }
        // read from file
        return segments.get(segNo).readFromOffset(offset);
    }
}