package fitness.utils;

import representation.PatternRepresentation;
import utils.ColoredText;

import java.io.IOException;
import java.util.*;
import java.nio.file.*;

import static utils.Utils.getRequiredProperty;
import static utils.Utils.loadConfig;
import fitness.fitnesstypes.FitnessFunctionEnum;

public class ScoreCalculator {

    public static double calculateFitnessScore(
            FitnessFunctionEnum fitnessFunction,
            Set<List<Map<String, Object>>> targetSequences,
            Set<List<Map<String, Object>>> detectedSequences,
            PatternRepresentation patternRepresentation,
            double beta) throws Exception {

        Set<String> serializedTargetSequences = serializeSequences(targetSequences);
        Set<String> serializedDetectedSequences = serializeSequences(detectedSequences);

        switch (fitnessFunction) {
            case FBETA:
                return calculateFBeta(serializedDetectedSequences, serializedTargetSequences, targetSequences, beta,
                        patternRepresentation);
            case BINARYEVENTS:
                return calculateBinaryFBeta(detectedSequences, targetSequences, beta, patternRepresentation);
            default:
                return 0.0;
        }
    }

    private static double calculateFBeta(
            Set<String> serializedDetectedSequences,
            Set<String> serializedTargetSequences,
            Set<List<Map<String, Object>>> targetSequences,
            double beta,
            PatternRepresentation patternRepresentation) {

        // Count True Positives and False Negatives

        int truePositives = (int) serializedTargetSequences.parallelStream()
        .filter(serializedDetectedSequences::contains)
        .count();

        int falseNegatives = (int) serializedTargetSequences.parallelStream()
                .filter(s -> !serializedDetectedSequences.contains(s))
                .count();

        int falsePositives = (int) serializedDetectedSequences.parallelStream()
                .filter(s -> !serializedTargetSequences.contains(s))
                .count();


        // Calculate Precision and Recall
        double precision = (truePositives + falsePositives == 0)
                ? 0.0
                : (double) truePositives / (truePositives + falsePositives);

        double recall = (targetSequences.isEmpty())
                ? 0.0
                : (double) truePositives / (truePositives + falseNegatives);

        // Calculate F-beta-score
        double betaSquared = beta * beta;
        double fBetaScore = (precision + recall == 0.0)
                ? 0.0
                : (1 + betaSquared) * (precision * recall) / ((betaSquared * precision) + recall);

        // Print individual and its fitness
        String configPath = System.getenv("CONFIG_PATH");
        Properties myConfig = null;
        try {
            myConfig = loadConfig(configPath);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String printIndividuals = utils.Utils.getRequiredProperty(myConfig, "printIndividuals");
        if (printIndividuals.equals("true")) {
            System.out.println(ColoredText.LIGHT_GRAY + patternRepresentation + ColoredText.RESET + ColoredText.ORANGE
                    + "[ScoreCalculator] Precision: " + precision + ", Recall: " + recall + ", F" + beta + "-Score: "
                    + fBetaScore + "\n" + ColoredText.RESET);
        }
        return fBetaScore;
    }

    private static double calculateBinaryFBeta(
        Set<List<Map<String, Object>>> detectedSequences,
        Set<List<Map<String, Object>>> targetSequences,
        double beta,
        PatternRepresentation patternRepresentation) {

    String configPath = System.getenv("CONFIG_PATH");
    Properties myConfig = null;
    try {
        myConfig = loadConfig(configPath);
    } catch (Exception e) {
        e.printStackTrace();
    }

    // Convert sequences into individual events (Set of Maps)
    Set<Map<String, Object>> targetEvents = new HashSet<>();
    Set<Map<String, Object>> detectedEvents = new HashSet<>();

    for (List<Map<String, Object>> sequence : targetSequences) {
        targetEvents.addAll(sequence);  // Aggiunge ogni evento singolarmente
    }
    for (List<Map<String, Object>> sequence : detectedSequences) {
        detectedEvents.addAll(sequence); // Aggiunge ogni evento singolarmente
    }

    // TP, FP, FN
    int truePositives = 0, falsePositives = 0, falseNegatives = 0;

    for (Map<String, Object> event : targetEvents) {
        if (detectedEvents.contains(event)) {
            truePositives++;
        } else {
            falseNegatives++;
        }
    }

    for (Map<String, Object> event : detectedEvents) {
        if (!targetEvents.contains(event)) {
            falsePositives++;
        }
    }

    if (truePositives == 0) {
        return 0.0;
    }

    // Precision, Recall and F-beta Score
    double precision = (double) truePositives / (truePositives + falsePositives);
    double recall = (double) truePositives / (truePositives + falseNegatives);
    double betaSquared = beta * beta;
    double fBetaScore = (1 + betaSquared) * (precision * recall) / (betaSquared * precision + recall);

    String printIndividuals = getRequiredProperty(myConfig, "printIndividuals");
    if ("true".equals(printIndividuals)) {
        System.out.println(ColoredText.LIGHT_GRAY + patternRepresentation + ColoredText.RESET + ColoredText.ORANGE + "[ScoreCalculator] (calculateBinaryFBeta) Precision: " + precision +
                ", Recall: " + recall + ", F" + beta + "-Score: " + fBetaScore + ColoredText.RESET);
    }

    return fBetaScore;
}

    private static List<String> readAndSerializeCsvFile(String filePath) {
        List<String> serializedDataset = new ArrayList<>();
        try {
            List<String> lines = Files.readAllLines(Paths.get(filePath));
            if (lines.isEmpty()) return serializedDataset;

            String[] headers = lines.get(0).split(",");

            for (int i = 1; i < lines.size(); i++) {
                String[] values = lines.get(i).split(",");
                if (values.length != headers.length) {
                    System.err.println("⚠️ Riga ignorata per mismatch feature-valori: " + lines.get(i));
                    continue;
                }

                Map<String, Object> eventMap = new HashMap<>();
                for (int j = 0; j < headers.length; j++) {
                    eventMap.put(headers[j].trim(), normalizeValue(values[j].trim()));
                }

                serializedDataset.add(serializeMap(eventMap) + ";");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return serializedDataset;
    }

    private static Object normalizeValue(String value) {
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return value.toLowerCase();
        }
        try {
            double num = Double.parseDouble(value);
            return (num == Math.floor(num)) ? String.format("%.0f", num) : String.format("%.6f", num);
        } catch (NumberFormatException e) {
            return value;
        }
    }

    private static Set<String> serializeSequences(Set<List<Map<String, Object>>> sequences) {
        Set<String> serializedSet = new HashSet<>();
        for (List<Map<String, Object>> sequence : sequences) {
            serializedSet.add(serializeSequence(sequence));
        }
        return serializedSet;
    }

    private static String serializeSequence(List<Map<String, Object>> sequence) {
        StringBuilder serialized = new StringBuilder();
        for (Map<String, Object> map : sequence) {
            serialized.append(serializeMap(map)).append(";");
        }
        return serialized.toString();
    }

    private static String serializeMap(Map<String, Object> map) {
        List<String> entries = new ArrayList<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = normalizeValue(entry.getValue().toString());
            entries.add(key + "=" + value);
        }
        entries.sort(String::compareTo);
        return String.join(",", entries);
    }
}
