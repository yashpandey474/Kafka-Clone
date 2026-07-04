package KafkaClone.src.main.java.broker;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class OffsetIndex {
    Map<Integer, Long> offsetIndex; // Offset -> byte number
    File indexFile; // Index: Store offset to byte

    public OffsetIndex(String partitionDirectoryName, int segmentNo) {
        this.indexFile = new File(partitionDirectoryName + "/segment-" + segmentNo + ".index");
    }

    void addEntry(int currentOffset, long currentFileLength) {
        // Store offset -> byte, message of this offset starts at this byte
        offsetIndex.put(currentOffset, currentFileLength);

        // Write to the index file
        String indexEntry = currentOffset + "," + currentFileLength + "\n"; // Make this binary later, maybe 2 functions
                                                                            // in MessageSerializer
        try (FileWriter fw = new FileWriter(indexFile, true)) { // use random access file
            fw.write(indexEntry);
        } catch (Exception e) {
            System.out.printf("Encountered error while writing to index file: %s. ERROR: %s", indexEntry,
                    e.getMessage());
        }
    }
    
    long lookupOffset(int offset) {
        return offsetIndex.get(offset);
    }
}
