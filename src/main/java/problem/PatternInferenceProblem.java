package problem;

import cep.TargetSequencesGenerator;
import events.BaseEvent;
import events.factory.DataStreamFactory;
import fitness.FitnessCalculator;
import fitness.fitnesstypes.FitnessFunctionEnum;
import fitness.utils.TargetSequenceReader;
import grammar.GrammarGenerator;
import grammar.datatypes.GrammarTypes;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.GrammarBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import representation.PatternRepresentation;
import representation.mappers.TreeToRepresentationMapper;
import utils.ColoredText;
import utils.CsvAnalyzer;

import java.util.*;
import java.util.function.Function;

import static utils.Utils.*;

public class PatternInferenceProblem implements GrammarBasedProblem<String, PatternRepresentation>, SimpleMOProblem<PatternRepresentation, Double> {
    private final Set<List<Map<String, Object>>> targetSequences;
    private final StringGrammar<String> grammar;
    private final FitnessFunctionEnum fitnessFunctionEnum;
    private final String csvFilePath;
    private final String targetDatasetPath;
    private final long duration;
    private final long numEvents;

    public PatternInferenceProblem(String configPath, FitnessFunctionEnum fitnessFunction) throws Exception {
        Properties myConfig = loadConfig(configPath);

        String datasetDirPath = getRequiredProperty(myConfig, "datasetDirPath");
        this.csvFilePath = datasetDirPath + getRequiredProperty(myConfig, "csvFileName");
        this.targetDatasetPath = getRequiredProperty(myConfig, "targetDatasetPath");
        this.duration = CsvAnalyzer.calculateDurationFromCsv(csvFilePath);
        this.numEvents = CsvAnalyzer.countRowsInCsv(csvFilePath);
        this.fitnessFunctionEnum = fitnessFunction;

        // Generate target sequences
        StreamExecutionEnvironment targetRemoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment(
                "jobmanager",
                8081,
                "/workspace/target/flinkCEP-Patterns-0.1-jar-with-dependencies.jar"
        );
        ExecutionConfig config = targetRemoteEnvironment.getConfig();
        targetRemoteEnvironment.getCheckpointConfig().disableCheckpointing();
        config.registerKryoType(java.util.HashMap.class);
        config.registerKryoType(BaseEvent.class);
        //System.out.println(ColoredText.YELLOW + "[PatternInferenceProblem] Creating target datastream." + ColoredText.RESET);
        DataStream<BaseEvent> eventStream = DataStreamFactory.createDataStream(targetRemoteEnvironment, csvFilePath);
        //System.out.println(ColoredText.YELLOW + "[PatternInferenceProblem] Creating target sequences." + ColoredText.RESET);
        TargetSequencesGenerator.saveMatchesToFile(
                TargetSequencesGenerator.createTargetPatterns(),
                eventStream,
                targetDatasetPath,
                myConfig.getProperty("targetKeyByField")
        );

        // Load target sequences
        //System.out.println(ColoredText.YELLOW + "[PatternInferenceProblem] Reading target sequences from file." + ColoredText.RESET);
        this.targetSequences = TargetSequenceReader.readTargetSequencesFromFile(targetDatasetPath);

        // Generate grammar
        String grammarFilePath = getRequiredProperty(myConfig, "grammarDirPath") + getRequiredProperty(myConfig, "grammarFileName");
        String type = getRequiredProperty(myConfig, "grammarType");
        GrammarTypes grammarType = GrammarTypes.valueOf(type);

        //System.out.println(ColoredText.YELLOW + "[PatternInferenceProblem] Creating grammar." + ColoredText.RESET);
        GrammarGenerator.generateGrammar(csvFilePath, grammarFilePath, grammarType, numEvents);
        this.grammar = loadGrammar(grammarFilePath);
    }

    @Override
    public Function<PatternRepresentation, SequencedMap<String, Double>> qualityFunction() {
        return patternRepresentation -> {
            try {
                StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment(
                        "jobmanager",
                        8081,
                        "/workspace/target/flinkCEP-Patterns-0.1-jar-with-dependencies.jar"
                );

                ExecutionConfig config = remoteEnvironment.getConfig();
                remoteEnvironment.getCheckpointConfig().disableCheckpointing();
                config.registerKryoType(java.util.HashMap.class);
                config.registerKryoType(BaseEvent.class);

                // Create datastream
                //System.out.println(ColoredText.YELLOW + "[PatternInferenceProblem] Creating datastream." + ColoredText.RESET);
                DataStream<BaseEvent> eventStream = DataStreamFactory.createDataStream(remoteEnvironment, csvFilePath);

                // Compute F1-Score on sequences
                //System.out.println(ColoredText.YELLOW + "[PatternInferenceProblem] Computing F1 Sequences." + ColoredText.RESET);
                FitnessCalculator fitnessCalculator = new FitnessCalculator(targetSequences);
                double fitnessValue1 = fitnessCalculator.calculateFitness(
                        FitnessFunctionEnum.FBETA,
                        remoteEnvironment,
                        eventStream,
                        new representation.mappers.RepresentationToPatternMapper<BaseEvent>().convert(patternRepresentation, duration, numEvents),
                        patternRepresentation.keyByClause(),
                        patternRepresentation
                );

                // Compute F1-Score on events
                //System.out.println(ColoredText.YELLOW + "[PatternInferenceProblem] Computing F1 Events." + ColoredText.RESET);
                double fitnessValue2 = fitnessCalculator.calculateFitness(
                        FitnessFunctionEnum.BINARYEVENTS,
                        remoteEnvironment,
                        eventStream,
                        new representation.mappers.RepresentationToPatternMapper<BaseEvent>().convert(patternRepresentation, duration, numEvents),
                        patternRepresentation.keyByClause(),
                        patternRepresentation
                );

                //System.out.println(ColoredText.YELLOW + "[PatternInferenceProblem] Finished computing F1 scores." + ColoredText.RESET);

                // Create map of fitness functions
                SequencedMap<String, Double> qualityMap = new LinkedHashMap<>();
                qualityMap.put("F1sequences", fitnessValue1);
                qualityMap.put("F1events", fitnessValue2);

                return qualityMap;
            } catch (Exception e) {
                e.printStackTrace();
                SequencedMap<String, Double> defaultMap = new LinkedHashMap<>();
                defaultMap.put("F1sequences", 0.0);
                defaultMap.put("F1events", 0.0);
                return defaultMap;
            }
        };
    }

    @Override
    public SequencedMap<String, Comparator<Double>> comparators() {
        SequencedMap<String, Comparator<Double>> comparatorMap = new LinkedHashMap<>();

        comparatorMap.put("F1sequences", Comparator.reverseOrder());
        comparatorMap.put("F1events", Comparator.reverseOrder());

        return comparatorMap;
    }

    @Override
    public StringGrammar<String> grammar() {
        return grammar;
    }

    @Override
    public Function<Tree<String>, PatternRepresentation> solutionMapper() {
        return new TreeToRepresentationMapper();
    }
}
