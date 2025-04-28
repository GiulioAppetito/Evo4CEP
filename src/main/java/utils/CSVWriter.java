package utils;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CSVWriter {

    public static void writeSequencesToCSV(String filePath, Set<List<Map<String, Object>>> sequences) throws IOException {
        try (FileWriter writer = new FileWriter(filePath)) {
            // Write header
            writer.append("Sequence Index,Event Index,Field,Value\n");

            int sequenceIndex = 1;
            for (List<Map<String, Object>> sequence : sequences) {
                int eventIndex = 1;
                for (Map<String, Object> event : sequence) {
                    for (Map.Entry<String, Object> entry : event.entrySet()) {
                        writer.append(String.valueOf(sequenceIndex))
                                .append(",")
                                .append(String.valueOf(eventIndex))
                                .append(",")
                                .append(entry.getKey())
                                .append(",")
                                .append(String.valueOf(entry.getValue()))
                                .append("\n");
                    }
                    eventIndex++;
                }
                sequenceIndex++;
            }
        }
    }
}
