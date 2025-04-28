package representation.mappers.treerepresentationparsers;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import representation.PatternRepresentation;

import java.util.ArrayList;
import java.util.List;

public class EventParser {

    public static List<PatternRepresentation.Event> parseEvents(Tree<String> eventsNode) {
        List<PatternRepresentation.Event> events = new ArrayList<>();

        if (eventsNode.nChildren() == 0) return events;

        Tree<String> firstEventNode = eventsNode.child(0);
        PatternRepresentation.Event firstEvent = parseSingleEvent(firstEventNode);
        events.add(firstEvent);

        if (eventsNode.nChildren() > 1) {
            Tree<String> concatNode = eventsNode.child(1);
            Tree<String> remainingEventsNode = eventsNode.child(2);

            PatternRepresentation.Event.Concatenator concatenator = ConcatenatorParser.parseConcatenator(concatNode);
            List<PatternRepresentation.Event> remainingEvents = parseEvents(remainingEventsNode);

            if (!remainingEvents.isEmpty()) {
                PatternRepresentation.Event firstRemainingEvent = remainingEvents.get(0);
                remainingEvents.set(0, new PatternRepresentation.Event(
                        firstRemainingEvent.identifier(),
                        firstRemainingEvent.conditions(),
                        firstRemainingEvent.quantifier(),
                        concatenator
                ));
            }
            events.addAll(remainingEvents);
        }
        return events;
    }

    public static PatternRepresentation.Event parseSingleEvent(Tree<String> eventNode) {
        String identifier = null;
        List<PatternRepresentation.Condition> conditions = new ArrayList<>();
        PatternRepresentation.Quantifier quantifier = null;

        for (Tree<String> child : eventNode) {
            switch (child.content()) {
                case "<identifier>":
                    identifier = child.visitLeaves().get(0);
                    break;
                case "<conditions>":
                    conditions = ConditionParser.parseConditions(child);
                    break;
                case "<quantifier>":
                    quantifier = QuantifierParser.parseQuantifier(child);
                    break;
            }
        }
        if (identifier == null) throw new IllegalArgumentException("Event identifier cannot be null");

        return new PatternRepresentation.Event(identifier, conditions, quantifier, null);
    }
}
