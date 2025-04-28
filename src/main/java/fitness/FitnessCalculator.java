package fitness;

import events.BaseEvent;
import fitness.fitnesstypes.FitnessFunctionEnum;
import fitness.utils.EventSequenceMatcher;
import fitness.utils.ScoreCalculator;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import representation.PatternRepresentation;
import utils.ColoredText;
import static utils.Utils.getRequiredProperty;
import static utils.Utils.loadConfig;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class FitnessCalculator {

    private final Set<List<Map<String, Object>>> targetSequences;
    private Map<String, Object> matchResults = null;

    private static long timeoutSeconds;

    // Separate caches for different fitness function types
    private static final Map<Integer, Double> fitnessCacheFbeta = new ConcurrentHashMap<>();
    private static final Map<Integer, Double> fitnessCacheBinaryEvents = new ConcurrentHashMap<>();

    private static final String CSV_FILE_PATH = generateDynamicFileName();

    public FitnessCalculator(Set<List<Map<String, Object>>> targetSequences) {
        this.targetSequences = targetSequences;
        String configPath = System.getenv("CONFIG_PATH");
        Properties myConfig = null;
        try {
            myConfig = loadConfig(configPath);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String timeout = utils.Utils.getRequiredProperty(myConfig, "timeout");
        timeoutSeconds = Long.parseLong(timeout);

    }

    /**
     * Generates a dynamic file name for storing fitness results, based on the current timestamp.
     */
    private static String generateDynamicFileName() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = dateFormat.format(new Date());
        return "RESULTS/" + timestamp + "_job_fitness_and_times.csv";
    }

    /**
     * Computes the fitness score of a pattern representation based on the selected fitness function.
     * Uses caching to avoid redundant computations.
     *
     * @param fitnessFunction The fitness function type (FBETA or BINARYEVENTS)
     * @param env Flink execution environment
     * @param inputDataStream The input data stream
     * @param generatedPattern The generated pattern to evaluate
     * @param keyByClause KeyBy clause used in pattern detection
     * @param patternRepresentation The pattern representation to evaluate
     * @return The computed fitness score
     * @throws Exception If an error occurs during fitness evaluation
     */
    public double calculateFitness(
                                   FitnessFunctionEnum fitnessFunction,
                                   StreamExecutionEnvironment env,
                                   DataStream<BaseEvent> inputDataStream,
                                   Pattern<BaseEvent, ?> generatedPattern,
                                   PatternRepresentation.KeyByClause keyByClause,
                                   PatternRepresentation patternRepresentation) throws Exception {

        int patternHash = patternRepresentation.hashCode();
        System.out.println("[FitnessCalculator] Timeout:"+timeoutSeconds);

        // Select the appropriate cache based on the fitness function
        Map<Integer, Double> selectedCache = (fitnessFunction == FitnessFunctionEnum.FBETA) 
                ? fitnessCacheFbeta 
                : fitnessCacheBinaryEvents;

        // Check if the fitness score is already cached
        if (selectedCache.containsKey(patternHash)) {
            System.out.println(ColoredText.GREEN + "Fitness cached for hash: " + patternHash + " (" + fitnessFunction + ")" + ColoredText.RESET);
            return selectedCache.get(patternHash);
        }

        // Create an event stream, with or without "keyBy"
        DataStream<BaseEvent> streamToUse = (keyByClause != null && keyByClause.key() != null)
                ? inputDataStream.keyBy(event -> event.toMap().get(keyByClause.key()))
                : inputDataStream;

        long time_a = System.nanoTime();
        // Perform event matching using the specified pattern
        if (matchResults==null){
            System.out.println(ColoredText.BLUE+"[FitnessCalculator] Calling EventSequenceMatcher."+ColoredText.RESET);
            EventSequenceMatcher matcher = new EventSequenceMatcher();
            this.matchResults = matcher.collectSequenceMatches(
                env, streamToUse, generatedPattern, "Generated", keyByClause, patternRepresentation, timeoutSeconds, TimeUnit.SECONDS);
        }
        long time_b = System.nanoTime();
        
        // Retrieve detected sequences from the matching process
        Set<List<Map<String, Object>>> detectedSequences = (Set<List<Map<String, Object>>>) matchResults.get("detectedSequences");
        String jobId = patternRepresentation.toString();
        double durationSeconds = (double) matchResults.get("durationSeconds");
        int foundMatches = (int) matchResults.get("foundMatches");
        boolean completedInTime = (boolean) matchResults.get("completedInTime");

        long time_c = System.nanoTime();
        // Compute the fitness score using the selected fitness function
        double beta = 1;
        double fitnessScore = ScoreCalculator.calculateFitnessScore(fitnessFunction, targetSequences, detectedSequences, patternRepresentation, beta);

        long time_d = System.nanoTime();
        // Store the computed fitness score in the appropriate cache
        selectedCache.put(patternHash, fitnessScore);

        // Save fitness data to a CSV file for logging purposes
        saveFitnessData(jobId, fitnessFunction, fitnessScore, durationSeconds, foundMatches, completedInTime);
        long time_e = System.nanoTime();

        //System.out.println();
        //System.out.println(ColoredText.CYAN + "[FitnessCalculator] Total time for individual evaluation: "+(time_e - time_a) / 1_000_000_000.0+"s");
        //System.out.println(ColoredText.CYAN + "[FitnessCalculator] Time to perform event matching: "+(time_b - time_a) / 1_000_000_000.0+"s");
        //System.out.println(ColoredText.CYAN + "[FitnessCalculator] Time to collect detected sequences: "+(time_c - time_b) / 1_000_000_000.0+"s");
        //System.out.println(ColoredText.CYAN + "[FitnessCalculator] Time to compute score: "+(time_d - time_c) / 1_000_000_000.0+"s");
        //System.out.println(ColoredText.CYAN + "[FitnessCalculator] Time to save fitness data: "+(time_e - time_d) / 1_000_000_000.0+"s");

        return fitnessScore;
    }

    /**
     * Saves fitness evaluation results to a CSV file for logging and analysis.
     *
     * @param jobId The unique identifier for the job/pattern
     * @param fitnessFunction The fitness function type used
     * @param fitness The computed fitness score
     * @param duration The duration taken to evaluate the pattern
     * @param matches The number of matches found
     */
    private void saveFitnessData(String jobId, FitnessFunctionEnum fitnessFunction, double fitness, double duration, int matches, boolean completedInTime) {
        try (FileWriter writer = new FileWriter(CSV_FILE_PATH, true)) {
            writer
                  .append(jobId).append("|")
                  .append(fitnessFunction.name()).append("|")
                  .append(String.format("%.3f", fitness)).append("|")
                  .append(String.format("%.3f", duration)).append("|")
                  .append(String.valueOf(matches)).append("|")
                  .append(String.valueOf(completedInTime)).append("\n");
            writer.flush();
        } catch (IOException e) {
            System.err.println("[ERROR] Could not save fitness data: " + e.getMessage());
        }
    }
}