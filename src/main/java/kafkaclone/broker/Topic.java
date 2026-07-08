package KafkaClone.src.main.java.kafkaclone.broker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import KafkaClone.src.main.java.kafkaclone.storage.Message;

// A topic is a logical category of messages
// Partitions are used for scalability so that multiple producers and consumers can write and read at same time repectively
public class Topic {
    String topicName;
    List<Partition> partitions;
    int messageLimitPerSegment;
    String topicDirectoryName;
    int numPartitions;

    // Round robin to partitions
    int currPartition;

    public Topic(String topicName, int numPartitions, int messageLimitPerSegment) {
        this.topicName = topicName;
        this.currPartition = 0;
        this.messageLimitPerSegment = messageLimitPerSegment;
        this.numPartitions = numPartitions;

        // create directory for the partition
        this.topicDirectoryName = "data/" + topicName;
        File topicsDirectory = new File(topicDirectoryName);
        topicsDirectory.mkdirs();

        // Initialise partitions
        initPartitions();
    }
    
    public void initPartitions() {
        partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            // initialise partition
            partitions.add(new Partition(i, messageLimitPerSegment, topicName, topicDirectoryName));
        }
    }

    public void addMessageToTopic(String key, String value) {
        // Publish a message to a particular partition
        System.out.printf("Message %s: %s being added to partition %d\n", key, value, currPartition);

        // Write to partition
        try {
            partitions.get(currPartition).addMessage(key, value);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            System.out.printf("Encountered an exception while adding message %s: %s to partition: %d of topic %s", key,
                    value, currPartition, topicName);
            e.printStackTrace();
            return;
        }
        
        // Round robin: next partition
        currPartition = (currPartition + 1) % partitions.size();
    }

    public List<Message> getMessages(int partitionNo, int offset) {
        if (partitionNo < 0 || partitionNo >= partitions.size()) {
            System.out.printf("Partition %d does not exist in topic %s \n", partitionNo, topicName);
            return null;
        }
        try {
            return partitions.get(partitionNo).getMessagesFromOffset(offset);
        } catch (NumberFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
}