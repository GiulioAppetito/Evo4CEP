package grammar.utils;

import grammar.datatypes.DataTypesEnum;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CSVTypesExtractor {

    public static Map<String, DataTypesEnum> getColumnTypesFromCSV(String csvFilePath, Set<String> allowedAttributes) throws IOException {
        Map<String, DataTypesEnum> columnTypes = new HashMap<>();
        try (Reader reader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            for (CSVRecord record : csvParser) {
                for (String column : record.toMap().keySet()) {
                    if (allowedAttributes != null && !allowedAttributes.isEmpty() && !allowedAttributes.contains(column)) {
                        continue; 
                    }

                    if (column.equalsIgnoreCase("timestamp")) {
                        continue;
                    }
                    String value = record.get(column);
                    DataTypesEnum currentType = inferType(value);
                    columnTypes.merge(column, currentType, (existingType, newType) -> {
                        if (existingType == DataTypesEnum.INT && newType == DataTypesEnum.DOUBLE) {
                            return DataTypesEnum.DOUBLE;
                        }
                        return existingType == newType ? existingType : DataTypesEnum.STRING;
                    });
                }
            }
        }
        return columnTypes;
    }

    public static Map<String, Set<String>> inferUniqueStringValues(String csvFilePath, Map<String, DataTypesEnum> columnTypes) throws IOException {
        Map<String, Set<String>> uniqueStringValues = new HashMap<>();
        try (Reader reader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            for (CSVRecord record : csvParser) {
                for (String column : record.toMap().keySet()) {
                    if (!columnTypes.containsKey(column)) {
                        continue; 
                    }
                    String value = record.get(column);
                    if (columnTypes.get(column) == DataTypesEnum.STRING) {
                        uniqueStringValues.computeIfAbsent(column, k -> new HashSet<>()).add(value);
                    }
                }
            }
        }
        return uniqueStringValues;
    }

    private static DataTypesEnum inferType(String value) {
        if (value.matches("-?\\d+")) return DataTypesEnum.INT;
        else if (value.matches("-?\\d*\\.\\d+([eE][-+]?\\d+)?")) return DataTypesEnum.DOUBLE;
        else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) return DataTypesEnum.BOOLEAN;
        else return DataTypesEnum.STRING;
    }

    public static int[] findIntegerBoundsFromCsv(String csvFilePath, Map<String, DataTypesEnum> columnTypes) throws IOException {
        int minInt = Integer.MAX_VALUE;
        int maxInt = Integer.MIN_VALUE;
    
        try (Reader reader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
    
            for (CSVRecord record : csvParser) {
                for (String column : columnTypes.keySet()) {
                    if (columnTypes.get(column) == DataTypesEnum.INT) { 
                        String value = record.get(column);
                        try {
                            int num = Integer.parseInt(value);
                            minInt = Math.min(minInt, num);
                            maxInt = Math.max(maxInt, num);
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            }
        }
    
        if (minInt == Integer.MAX_VALUE || maxInt == Integer.MIN_VALUE) {
            return new int[]{1, 100};
        }
    
        System.out.println("Integer bounds: [" + minInt + ", " + maxInt + "]");
        return new int[]{minInt, maxInt};
    }

    public static double[] findFloatBoundsFromCsv(String csvFilePath, Map<String, DataTypesEnum> columnTypes) throws IOException {
        double minFloat = Double.MAX_VALUE;
        double maxFloat = Double.MIN_VALUE;
    
        try (Reader reader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
    
            for (CSVRecord record : csvParser) {
                for (String column : columnTypes.keySet()) {
                    if (columnTypes.get(column) == DataTypesEnum.DOUBLE) { // Only process floating-point columns
                        String value = record.get(column);
                        try {
                            double num = Double.parseDouble(value);
                            minFloat = Math.min(minFloat, num);
                            maxFloat = Math.max(maxFloat, num);
                        } catch (NumberFormatException e) {
                            // Ignore invalid values
                        }
                    }
                }
            }
        }
    
        if (minFloat == Double.MAX_VALUE || maxFloat == Double.MIN_VALUE) {
            // No valid float values found, use a default range
            return new double[]{0.0, 100.0};
        }
    
        System.out.println("Float bounds: [" + minFloat + ", " + maxFloat + "]");
        return new double[]{minFloat, maxFloat};
    }
    
    
}
