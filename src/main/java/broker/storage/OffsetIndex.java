package KafkaClone.src.main.java.broker.storage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class OffsetIndex {
    Map<Integer, Long> offsetIndex; // Offset -> byte number
    File indexFile; // Index: Store offset to byte

    public OffsetIndex(String partitionDirectoryName, int segmentNo) {
        loadMapFromFile();
        this.indexFile = new File(partitionDirectoryName + "/segment-" + segmentNo + ".index");
    }

    String createIndexEntry(int currentOffset, long currentFileLength) {
        // Write to the index file
        return currentOffset + "," + currentFileLength + "\n"; // Make this binary later, maybe 2 functions in MessageSerializer
    }

    void addEntry(int currentOffset, long currentFileLength) {
        // Store offset -> byte, message of this offset starts at this byte
        offsetIndex.put(currentOffset, currentFileLength);
        String indexEntry = createIndexEntry(currentOffset, currentFileLength);
        try (FileWriter fw = new FileWriter(indexFile, true)) { // use random access file
            fw.write(indexEntry);
        } catch (Exception e) {
            System.out.printf("Encountered error while writing to index file: %s. ERROR: %s", indexEntry,
                    e.getMessage());
        }
    }
    
    long lookupOffset(int offset) {
        if (!offsetIndex.containsKey(offset)) {
            System.out.printf("Offset requested %d not present in the map", offset);
            return -1;
        }
        return offsetIndex.get(offset);
    }

    void loadMapFromFile() {
        try (RandomAccessFile raf = new RandomAccessFile(indexFile, "r")) {
            String line;
            // Read until the end of the file
            while ((line = raf.readLine()) != null) {
                String[] parts = line.split(",");

                // Create a new entry
                int offset = Integer.parseInt(parts[0].trim());
                long byteNo = Long.parseLong(parts[1].trim());
                offsetIndex.put(offset, byteNo);
            }

        } catch (IOException e) {
            System.out.printf(
                    "Encountered error while reading from file %s from offset %d. Error: %s%n",
                    indexFile.getName(),
                    e.getMessage());
        }
    }

}
