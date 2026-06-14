package KafkaClone.src.main.java.broker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// A topic is a logical category of messages
// Partitions are used for scalability so that multiple producers and consumers can write and read at same time repectively
public class Topic {
    String topicName;
    List<Partition> partitions;

    // Round robin to partitions
    int currPartition;

    public Topic(String topicName, Integer numPartitions) {
        this.topicName = topicName;
        this.currPartition = 0;

        // create directory for the partition
        File topicsDirectory = new File(
            "data/" + topicName
        );
        topicsDirectory.mkdirs();

        partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            // create a log file for the partition
            String fileName = "data/" + topicName + "/partition-" + i + ".log";
            File file = new File(fileName);

            // initialise partition
            partitions.add(new Partition(i, file, fileName));
        }
    }

    public void addMessageToTopic(String message) {
        // Publish a message to a particular partition
        System.out.printf("Message %s being added to partition %d\n", message, currPartition);

        //Create message object
        partitions.get(currPartition).createAndAddMessage(message);;
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