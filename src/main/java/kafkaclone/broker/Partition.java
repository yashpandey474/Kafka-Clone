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
    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);

    public Partition(int partitionNo, int messageLimitPerSegment, String topicName, String topicDirectoryName) {

        this.partitionNo = partitionNo;
        this.messageLimitPerSegment = messageLimitPerSegment;

        this.currentOffset = 0;

        this.partitionDirectoryName = topicDirectoryName + "/partition-" + partitionNo;
        File partitionDirectory = new File(partitionDirectoryName);
        partitionDirectory.mkdirs();

        // read all segment files in the directory and create logsegments
        this.recover();
    }

    public void recover() {
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
                                        .replace(".log", ""));
                        segmentNumbers.add(segmentNo);
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }

        LogSegment segment;
        this.segments = new ArrayList<>();

        // if there are no segments in the directory, create a segment and assign it as the active one
        if (segmentNumbers.size() == 0) {
            segment = new LogSegment(0, messageLimitPerSegment, partitionDirectoryName, 0);
            activeSegment = segment;
            segments.add(segment);
            return;
        }

        // sort so that the last segment is the active segment
        segmentNumbers.sort(null);

        for (int i = 0; i < segmentNumbers.size(); i++) {
            segment = new LogSegment(messageLimitPerSegment * segmentNumbers.get(i), messageLimitPerSegment,
                    partitionDirectoryName, segmentNumbers.get(i));
            segments.add(segment);
        }
        activeSegment = segments.get(segments.size() - 1);
    }


    // Recovery could load random number of segments, check active segment
    public int getNextSegmentNo() {
        return this.activeSegment.getSegmentNo() + 1;
    }
    
    // TODO: optimize with binary search
    public int getSegmentNoForOffset(int offset) {
        int segNo = -1;
        for (int i = 0; i < segments.size(); i++) {
            if (segments.get(i).getBaseOffset() > offset) {
                break;
            }
            segNo = i;
        }

        return segNo;
    }

    public int getCurrentOffset() {
        return activeSegment.getCurrentOffset();
    }

    // Partition adds message to its actual message queue
    public void addMessage(String key, String value) throws IOException { 
        // Initialise the new segment
        if (activeSegment.isFull()) {
            this.activeSegment = new LogSegment(getCurrentOffset(), messageLimitPerSegment, partitionDirectoryName,
                    segments.size());
            segments.add(activeSegment);
        }
        activeSegment.writeMessage(key, value);
    }

    // Fetch messages from a particular offset => correct kafka design to reduce latency and increase throughput
    public List<Message> getMessagesFromOffset(int offset) throws NumberFormatException, IOException {
        // figure out correct segment
        int segNo = getSegmentNoForOffset(offset);
        if (segNo < 0 || segNo >= segments.size()) {
            logger.error("Offset {} is out of range {} - {}", offset, 0, segments.size());
            return null;
        }

        // read through segments
        List<Message> messages = segments.get(segNo).readFromOffset(offset);
        for (int i = segNo + 1; i < segments.size(); i++) {
            messages.addAll(segments.get(i).readFromOffset(0));
        }
        return messages;
    }
}