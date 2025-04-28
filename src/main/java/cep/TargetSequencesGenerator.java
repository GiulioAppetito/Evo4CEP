package cep;

import events.BaseEvent;
import utils.ColoredText;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import static utils.Utils.loadConfig;

public class TargetSequencesGenerator {

    private static final Logger logger = LoggerFactory.getLogger(TargetSequencesGenerator.class);

    public static List<Pattern<BaseEvent, ?>> createTargetPatterns() throws Exception {
        List<Pattern<BaseEvent, ?>> targetPatterns = new ArrayList<>();

        String configPath = System.getenv("CONFIG_PATH");
        Properties myConfig = loadConfig(configPath);
        String targetStrategy = myConfig.getProperty("targetStrategy", "");
        long targetWithinWindowSeconds = Long.parseLong(myConfig.getProperty("targetWithinWindowSeconds", ""));
        long targetFromTimes = Long.parseLong(myConfig.getProperty("targetFromTimes", ""));
        long targetToTimes = Long.parseLong(myConfig.getProperty("targetToTimes", ""));

        AfterMatchSkipStrategy skipStrategy = switch (targetStrategy) {
            case "noSkip" -> AfterMatchSkipStrategy.noSkip();
            case "skipToNext" -> AfterMatchSkipStrategy.skipToNext();
            case "skipPastLastEvent" -> AfterMatchSkipStrategy.skipPastLastEvent();
            default -> throw new IllegalArgumentException("Invalid AfterMatchSkipStrategy: " + targetStrategy);
        };

        System.out.println(ColoredText.GREEN + "[TargetSequencesGenerator] Selected Target AfterMatchSkipStrategy: }" + skipStrategy);
        System.out.println("[TargetSequencesGenerator] targetWithinWindowSeconds is: "+ targetWithinWindowSeconds);
        System.out.println("[TargetSequencesGenerator] targetTimes is: "+ targetFromTimes +","+targetToTimes + ColoredText.RESET);

        Pattern<BaseEvent, ?> loginPattern = Pattern.<BaseEvent>begin("failed_login")
        .where(new SimpleCondition<>() {
            @Override
            public boolean filter(BaseEvent event) {
                Map<String, Object> eventMap = event.toMap();
                Boolean successfulLogin = (Boolean) eventMap.get("successful_login");
                
                boolean isFailedLogin = successfulLogin != null && !successfulLogin;
                
                if (isFailedLogin) {
                    System.out.println("Failed login detected: "+ eventMap);
                }
                
                return isFailedLogin;
            }
        })
        .times((int)targetFromTimes, (int)targetToTimes)  
        .within(Duration.ofSeconds(targetWithinWindowSeconds)); 

        Pattern<BaseEvent, ?> loginPattern3 = Pattern.<BaseEvent>begin("failed_login")
        .where(new SimpleCondition<>() {
            @Override
            public boolean filter(BaseEvent event) {
                Map<String, Object> eventMap = event.toMap();
                Boolean successfulLogin = (Boolean) eventMap.get("successful_login");
                
                boolean isFailedLogin = successfulLogin != null && !successfulLogin;
                
                return isFailedLogin;
            }
        });
        Pattern<BaseEvent, ?> pollutionHeatPattern = Pattern.<BaseEvent>begin("high_pollution")
            .where(new SimpleCondition<>() {
                @Override
                public boolean filter(BaseEvent event) {
                    Map<String, Object> eventMap = event.toMap();
                    return (Double) eventMap.get("co") > 0.0045 &&
                        (Double) eventMap.get("smoke") > 0.02;
                }
            }).times(2) 
            .followedBy("temp_rise")
            .where(new SimpleCondition<>() {
                @Override
                public boolean filter(BaseEvent event) {
                    Map<String, Object> eventMap = event.toMap();
                    return (Double) eventMap.get("temp") > 25.0;
                }
            })
            .within(Duration.ofSeconds(targetWithinWindowSeconds)); 

            
        Pattern<BaseEvent, ?> loginPattern1 = Pattern.<BaseEvent>begin("failed_logins", skipStrategy)
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(BaseEvent event) {
                        Map<String, Object> eventMap = event.toMap();
                        Object successful_login = eventMap.get("successful_login");

                        return Boolean.FALSE.equals(successful_login);
                    }
                }).oneOrMore() 
                .next("successful_login") 
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(BaseEvent event) {
                        Map<String, Object> eventMap = event.toMap();
                        Object successful_login = eventMap.get("successful_login");

                        return Boolean.TRUE.equals(successful_login);
                    }
                }).within(Duration.ofSeconds(targetWithinWindowSeconds));

                Pattern<BaseEvent, ?> numericPattern = Pattern.<BaseEvent>begin("temperature_spike")
                .where(new SimpleCondition<BaseEvent>() {
                    @Override
                    public boolean filter(BaseEvent event) {
                        Map<String, Object> eventMap = event.toMap();
                        double temperature = (double) eventMap.get("temperature");
                        return temperature > 30.0; 
                    }
                })
                .times((int)targetFromTimes, (int)targetToTimes);
            
        targetPatterns.add(pollutionHeatPattern);
        return targetPatterns;
    }

    public static void saveMatchesToFile(List<Pattern<BaseEvent, ?>> patterns, DataStream<BaseEvent> inputDataStream, String targetDatasetPath, String keyByField) throws Exception {

        DataStream<BaseEvent> loggedStream = inputDataStream.process(new ProcessFunction<BaseEvent, BaseEvent>() {
            @Override
            public void processElement(BaseEvent event, Context context, Collector<BaseEvent> collector) throws Exception {
                logger.debug("Received event: {}", event.toMap());
                collector.collect(event);
            }
        });

        DataStream<BaseEvent> streamToUse;
        if (keyByField != null && !keyByField.isEmpty()) {
            System.out.println(ColoredText.GREEN + "[TargetSequenceGenerator] Applying key_by with key: "+ keyByField + ColoredText.RESET);
            streamToUse = loggedStream.keyBy((KeySelector<BaseEvent, Object>) event -> event.toMap().get(keyByField));
        } else {
            System.out.println(ColoredText.GREEN +"[TargetSequenceGenerator] NOT applying key_by."+ ColoredText.RESET);
            streamToUse = loggedStream;
        }

        int length = 0;
        
        try (FileWriter writer = new FileWriter(targetDatasetPath)) {
            for (Pattern<BaseEvent, ?> pattern : patterns) {
                PatternStream<BaseEvent> patternStream = CEP.pattern(streamToUse, pattern);
                DataStream<List<BaseEvent>> matchedStream = patternStream.select(new LoggingPatternSelectFunction());
                
                Iterator<List<BaseEvent>> iterator = DataStreamUtils.collect(matchedStream, "Target Sequences Collection");
                while (iterator.hasNext()) {
                    List<BaseEvent> eventsList = iterator.next();
                    List<Map<String, Object>> sequence = new ArrayList<>();
                    for (BaseEvent event : eventsList) {
                        sequence.add(new HashMap<>(event.toMap()));
                    }
                    writer.write(sequenceToCsvLine(sequence) + "\n");
                    length++;
                }
                System.out.println(ColoredText.GREEN + "[TargetSequencesGenerator]: Finished writing the target file with "+ length +" matches." + ColoredText.RESET);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing matched sequences to file", e);
        }
    }

    private static String sequenceToCsvLine(List<Map<String, Object>> sequence) {
        StringBuilder builder = new StringBuilder();
        for (Map<String, Object> map : sequence) {
            builder.append(map.toString().replace(",", ";")).append("|");
        }
        return builder.toString();
    }

    private static class LoggingPatternSelectFunction implements PatternSelectFunction<BaseEvent, List<BaseEvent>> {
        @Override
        public List<BaseEvent> select(Map<String, List<BaseEvent>> match) {
            logger.info("### MATCH FOUND ###");
            for (Map.Entry<String, List<BaseEvent>> entry : match.entrySet()) {
                logger.info("Pattern: {}", entry.getKey());
                for (BaseEvent event : entry.getValue()) {
                    logger.info("Event in match: {}", event.toMap());
                }
            }
            logger.info("###################");
            List<BaseEvent> collectedEvents = new ArrayList<>();
            match.values().forEach(collectedEvents::addAll);
            return collectedEvents;
        }
    }
}
