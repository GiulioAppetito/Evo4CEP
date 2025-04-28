package fitness.utils;

import events.BaseEvent;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import representation.PatternRepresentation;
import utils.ColoredText;

import java.util.concurrent.*;
import java.util.*;

public class EventSequenceMatcher {

    private static final String CSV_FILE_PATH = "/RESULTS/job_times.csv";

    public Map<String, Object> collectSequenceMatches(StreamExecutionEnvironment remoteEnvironment,
                                                      DataStream<BaseEvent> inputDataStream,
                                                      Pattern<BaseEvent, ?> generatedPattern,
                                                      String type,
                                                      PatternRepresentation.KeyByClause keyByClause,
                                                      PatternRepresentation patternRepresentation,
                                                      long timeoutSeconds,
                                                      TimeUnit timeUnit) throws Exception {

        long startTime = System.nanoTime();

        DataStream<BaseEvent> keyedStream = (keyByClause != null && keyByClause.key() != null)
                ? inputDataStream.keyBy(event -> event.toMap().get(keyByClause.key()))
                : inputDataStream;

        DataStream<List<Map<String, Object>>> matchedStream = applyPatternToDatastream(keyedStream, generatedPattern);
        Set<List<Map<String, Object>>> detectedSequences = new HashSet<>();
        String jobName = patternRepresentation.toString();

        int foundMatches = 0;
        boolean completedInTime;
        double jobRuntimeSeconds = 0.0;

        ExecutorService executor = Executors.newSingleThreadExecutor();
        JobClient jobClient = remoteEnvironment.executeAsync(jobName);
        JobID jobId = jobClient.getJobID();

        Future<Integer> future = executor.submit(() -> {
            int matchCount = 0;
            try (CloseableIterator<List<Map<String, Object>>> iterator = matchedStream.executeAndCollect(jobName)) {
                while (iterator.hasNext()) {
                    detectedSequences.add(iterator.next());
                    matchCount++;
                }
            }
            System.out.println(ColoredText.PURPLE + "[EventSequenceMatcher] numMatches: " + matchCount + ColoredText.RESET);
            return matchCount;
        });

        try {
            foundMatches = future.get(timeoutSeconds, timeUnit);
            JobExecutionResult jobResult = jobClient.getJobExecutionResult().get();
            jobRuntimeSeconds = jobResult.getNetRuntime(TimeUnit.MILLISECONDS) / 1000.0;
            System.out.println(ColoredText.PURPLE + "[EventSequenceMatcher] Job completed in "+jobRuntimeSeconds + "s." + ColoredText.RESET);
            completedInTime = true;
        } catch (TimeoutException e) {
            System.err.println(ColoredText.RED + "[ERROR] Timeout (" + timeoutSeconds + " seconds) reached! Cancelling job " + jobId + "..." + ColoredText.RESET);
            if (jobClient.getJobStatus().get() != org.apache.flink.api.common.JobStatus.FINISHED) {
                jobClient.cancel().get();
            }            
            future.cancel(true);
            detectedSequences.clear();
            completedInTime = false;
        } finally {
            executor.shutdown();
        }

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;

        Map<String, Object> result = new HashMap<>();
        result.put("detectedSequences", detectedSequences);
        if(jobRuntimeSeconds==0.0){
            result.put("durationSeconds", durationSeconds);
        }else{
            result.put("durationSeconds", jobRuntimeSeconds);
        }
        
        result.put("foundMatches", foundMatches);
        result.put("completedInTime", completedInTime);

        return result;
    }

    private DataStream<List<Map<String, Object>>> applyPatternToDatastream(
            DataStream<BaseEvent> inputDataStream,
            Pattern<BaseEvent, ?> pattern) {

        PatternStream<BaseEvent> patternStream = CEP.pattern(inputDataStream, pattern);
        return patternStream.select(new PatternToListSelectFunction());
    }

    private static class PatternToListSelectFunction
            implements PatternSelectFunction<BaseEvent, List<Map<String, Object>>> {
        @Override
        public List<Map<String, Object>> select(Map<String, List<BaseEvent>> match) {
            List<Map<String, Object>> resultSequence = new ArrayList<>();
            for (List<BaseEvent> events : match.values()) {
                for (BaseEvent event : events) {
                    resultSequence.add(new HashMap<>(event.toMap()));
                }
            }
            return resultSequence;
        }
    }
}