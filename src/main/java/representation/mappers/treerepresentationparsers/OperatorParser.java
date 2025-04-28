package representation.mappers.treerepresentationparsers;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import representation.PatternRepresentation;

public class OperatorParser {

    public static PatternRepresentation.Condition.Operator parseOperator(Tree<String> opNode) {
        String value = opNode.visitLeaves().get(0);
        return switch (value) {
            case "equal" -> PatternRepresentation.Condition.Operator.EQUAL;
            case "notEqual" -> PatternRepresentation.Condition.Operator.NOT_EQUAL;
            case "lt" -> PatternRepresentation.Condition.Operator.LESS_THAN;
            case "gt" -> PatternRepresentation.Condition.Operator.GREATER_THAN;
            default -> throw new IllegalArgumentException("Unknown operator: " + value);
        };
    }
}
