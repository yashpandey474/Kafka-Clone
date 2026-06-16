package KafkaClone.src.main.java.broker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

// Partition is what actually holds the messages for a particular topic
public class Partition {
    int partitionNo;
    List<LogSegment> segments;
    int messageLimitPerSegment;
    int currentOffset;

    public Partition(int partitionNo, int messageLimitPerSegment) {
        this.partitionNo = partitionNo;
        this.messageLimitPerSegment = messageLimitPerSegment;
        this.segments = new ArrayList<>(); // need to implement recovery - read from existing files based on partitionNo and a base director
        this.currentOffset = 0;
    }

    public int getCurrentOffset() {
        return this.currentOffset;
    }

    public void setCurrentOffset(int offset) {
        this.currentOffset = offset;
    }

    // Partition adds message to its actual message queue
    public void addMessage(Message message) throws IOException { 
        // Choose correct logsegment based on current offset?
        int segmentNo = (currentOffset / messageLimitPerSegment);

        // Initialise the new segment
        if (segmentNo == this.segments.size()) {
            segments.add(new LogSegment(messageLimitPerSegment*segmentNo, segmentNo))
        } else if (segmentNo > this.segments.size()){
            System.out.printf("Segment No: %d is greater than size of segments %d. Missing segment", segmentNo, this.segments.size());
            return;
        }

        segments.get(segmentNo).writeMessage(message);
        currentOffset++;
    }

    // Fetch messages from a particular offset => correct kafka design to reduce latency and increase throughput
    public List<Message> getMessagesFromOffset(int offset) throws NumberFormatException, IOException {
        // read from file
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

    public void createAndAddMessage(String message) {
        Message m = new Message(message, messages.size());
        try {
            addMessage(m);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}