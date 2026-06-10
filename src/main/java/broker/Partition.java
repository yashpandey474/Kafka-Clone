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
    List<Message> messages;
    File logFile;
    String fileName;

    public Partition(int partitionNo, File logfile, String fileName) {
        this.partitionNo = partitionNo;
        this.messages = new ArrayList<>();
        this.logFile = logfile;
        this.fileName = fileName;
    }

    // Partition adds message to its actual message queue
    public void addMessage(Message message) throws IOException { 
        // open file in append mode
        FileWriter writer = null;
        try{
           writer = new FileWriter(logFile, true);
        } catch (Exception e) {
            if (writer != null) {
                writer.close();
            }
            System.out.printf("Encountered an error while writing message to file %s: %s", fileName, e.getMessage());
            return;
        }
        writer.write(message.offset + ":" + message.content + "\n");
        writer.close();
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
        addMessage(m);
    }
}