package fitness.utils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TargetSequenceReader {

    public static Set<List<Map<String, Object>>> readTargetSequencesFromFile(String filePath) {
        Set<List<Map<String, Object>>> sequences = new HashSet<>();
        try (Scanner scanner = new Scanner(new File(filePath))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                sequences.add(parseCsvLineToSequence(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sequences;
    }

    private static List<Map<String, Object>> parseCsvLineToSequence(String line) {
        List<Map<String, Object>> sequence = new ArrayList<>();
        String[] eventStrings = line.split("\\|");
        for (String eventString : eventStrings) {
            Map<String, Object> eventMap = new HashMap<>();
            String[] keyValuePairs = eventString.replace("{", "").replace("}", "").replace(";", ",").split(",");
            for (String pair : keyValuePairs) {
                String[] keyValue = pair.split("=");
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();
                    eventMap.put(key, inferValueType(value));
                }
            }
            sequence.add(eventMap);
        }
        return sequence;
    }

    private static Object inferValueType(String value) {
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }
        try {
            // Rileva numeri decimali con precisione esatta
            if (value.contains(".")) {
                return Double.valueOf(value); // Usa Double invece di Float
            }
            return Long.valueOf(value); // Per numeri interi
        } catch (NumberFormatException e) {
            return value; // Tratta come stringa se non è un numero
        }
    }

}
