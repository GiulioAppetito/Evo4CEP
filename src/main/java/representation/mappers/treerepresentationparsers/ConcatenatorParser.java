package representation.mappers.treerepresentationparsers;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import representation.PatternRepresentation;

public class ConcatenatorParser {

    public static PatternRepresentation.Event.Concatenator parseConcatenator(Tree<String> concatNode) {
        String value = concatNode.visitLeaves().get(0);
        return switch (value) {
            case "next" -> PatternRepresentation.Event.Concatenator.NEXT;
            case "followedBy" -> PatternRepresentation.Event.Concatenator.FOLLOWED_BY;
            case "followedByAny" -> PatternRepresentation.Event.Concatenator.FOLLOWED_BY_ANY;
            case "notNext"-> PatternRepresentation.Event.Concatenator.NOT_NEXT;
            case "notFollowedBy"-> PatternRepresentation.Event.Concatenator.NOT_FOLLOWED_BY;
            default -> throw new IllegalArgumentException("Unknown concatenator: " + value);
        };
    }

    public static PatternRepresentation.Condition.Concatenator parseConditionConcatenator(Tree<String> concatNode) {
        String value = concatNode.visitLeaves().get(0);
        return switch (value) {
            case "and" -> PatternRepresentation.Condition.Concatenator.AND;
            case "or" -> PatternRepresentation.Condition.Concatenator.OR;
            default -> throw new IllegalArgumentException("Unknown condition concatenator: " + value);
        };
    }
}
