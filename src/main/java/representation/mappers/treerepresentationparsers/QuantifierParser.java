package representation.mappers.treerepresentationparsers;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import representation.PatternRepresentation;

import java.util.Map;

public class QuantifierParser {

    public static PatternRepresentation.Quantifier parseQuantifier(Tree<String> quantifierNode) {
        Tree<String> quantNode = quantifierNode.child(0); // Identify quantifier type
        return switch (quantNode.content()) {
            case "oneOrMore" -> PatternRepresentation.Quantifier.ParamFree.ONE_OR_MORE;
            case "optional" -> PatternRepresentation.Quantifier.ParamFree.OPTIONAL;
            case "times" -> {
                // Parse the number of times from <greaterThanZeroNum>
                Tree<String> timesValueNode = quantifierNode.child(1);
                long timesValue = parseGreaterThanZeroNum(timesValueNode);
                yield new PatternRepresentation.Quantifier.NTimes(timesValue);
            }
            case "range" -> {
                Tree<String> fromNode = quantifierNode.child(1);
                Tree<String> toNode = quantifierNode.child(2);
                long fromValue = parseGreaterThanZeroNum(fromNode);
                long toValue = parseGreaterThanZeroNum(toNode);
                // Re-order the two values for a meaningful range
                yield new PatternRepresentation.Quantifier.FromToTimes(Math.min(fromValue,toValue), Math.max(fromValue,toValue));
            }
            default -> throw new IllegalArgumentException("Unknown quantifier: " + quantNode.content());
        };
    }

    private static long parseGreaterThanZeroNum(Tree<String> numNode) {
        // Traverse and build the number by visiting all leaf nodes and extracting their content
        StringBuilder numberBuilder = new StringBuilder();
        for (String leafNode : numNode.visitLeaves()) {
            numberBuilder.append(leafNode);
        }
        return Long.parseLong(numberBuilder.toString());
    }
}
