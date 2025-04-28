package representation.mappers;

import events.BaseEvent;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import representation.PatternRepresentation;
import representation.mappers.utils.SimpleEventCondition;
import utils.ColoredText;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static utils.Utils.loadConfig;

public class RepresentationToPatternMapper<E extends BaseEvent> {

    // Converts PatternRepresentation to a Flink Pattern
    public Pattern<E, ?> convert(PatternRepresentation representation, long duration, long numEvents) {
        List<PatternRepresentation.Event> events = representation.events();
        Pattern<E, E> flinkPattern = null;
        Map<String, Integer> eventNameCounts = new HashMap<>(); // Track counts for unique names

        for (int i = 0; i < events.size(); i++) {
            PatternRepresentation.Event event = events.get(i);

            // Ensure unique event identifier with suffix
            String baseIdentifier = event.identifier();
            int count = eventNameCounts.getOrDefault(baseIdentifier, 0) + 1;
            eventNameCounts.put(baseIdentifier, count);
            String uniqueIdentifier = baseIdentifier + "_" + count;

            Pattern<E, E> newPattern = null; // Use unique identifier
            try {
                newPattern = createPatternForEvent(event, uniqueIdentifier, numEvents);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            // Initialize with the first event, otherwise chain the events
            if (flinkPattern == null) {
                flinkPattern = newPattern;
            } else {
                PatternRepresentation.Event.Concatenator concatenator = events.get(i - 1).concatenator();
                if (concatenator != null) {
                    flinkPattern = switch (concatenator) {
                        case NEXT -> flinkPattern.next(newPattern);
                        case FOLLOWED_BY -> flinkPattern.followedBy(newPattern);
                        case FOLLOWED_BY_ANY -> flinkPattern.followedByAny(newPattern);
                        case NOT_NEXT -> flinkPattern.notNext(uniqueIdentifier);
                        case NOT_FOLLOWED_BY -> flinkPattern.notFollowedBy(uniqueIdentifier);
                    };
                } else {
                    flinkPattern = flinkPattern.next(newPattern);
                }
            }
        }

        // Apply within clause if specified
        if (representation.withinClause() != null) {
            long actualDuration = Math.min(duration, (long) representation.withinClause().duration());
            flinkPattern = flinkPattern.within(Duration.ofMillis(actualDuration));
        }

        return flinkPattern;
    }

    // Creates a Pattern for a single event, applying any conditions and quantifiers
    private Pattern<E, E> createPatternForEvent(PatternRepresentation.Event event, String uniqueIdentifier, long numEvents) throws Exception {
        String configPath = System.getenv("CONFIG_PATH");
        Properties myConfig = loadConfig(configPath);
        String individualStrategy = myConfig.getProperty("individualStrategy", "");
        AfterMatchSkipStrategy skipStrategy = switch (individualStrategy) {
            case "noSkip" -> AfterMatchSkipStrategy.noSkip();
            case "skipToNext" -> AfterMatchSkipStrategy.skipToNext();
            case "skipPastLastEvent" -> AfterMatchSkipStrategy.skipPastLastEvent();
            default -> throw new IllegalArgumentException("Invalid AfterMatchSkipStrategy: " + individualStrategy);
        };
        Pattern<E, E> pattern = Pattern.<E>begin(uniqueIdentifier, skipStrategy); // Use unique identifier

        if (event.quantifier() instanceof PatternRepresentation.Quantifier.ParamFree quantifier) {
            pattern = switch (quantifier) {
                case ONE_OR_MORE -> pattern.oneOrMore();
                case OPTIONAL -> pattern.optional();
            };
        } else if (event.quantifier() instanceof PatternRepresentation.Quantifier.NTimes nTimes) {
            long actualTimes = Math.min(nTimes.n(), numEvents);
            pattern = pattern.times((int)actualTimes);
        } else if (event.quantifier() instanceof PatternRepresentation.Quantifier.FromToTimes fromToTimes) {
            long from = fromToTimes.from();
            long size = fromToTimes.to();
            long to = from + size;

            if(to == from){
                pattern = pattern.times((int)to);
            }else{
                pattern = pattern.times((int)from, (int)to);
            }
        }

        // Attach conditions to the pattern
        for (PatternRepresentation.Condition condition : event.conditions()) {
            pattern = pattern.where(new SimpleEventCondition<>(condition));
        }

        return pattern;
    }

}
