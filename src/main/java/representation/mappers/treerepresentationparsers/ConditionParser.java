package representation.mappers.treerepresentationparsers;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import representation.PatternRepresentation;

import java.util.ArrayList;
import java.util.List;

public class ConditionParser {

    public static List<PatternRepresentation.Condition> parseConditions(Tree<String> conditionsNode) {
        List<PatternRepresentation.Condition> conditions = new ArrayList<>();

        if (conditionsNode.nChildren() == 0) return conditions;

        Tree<String> firstConditionNode = conditionsNode.child(0);
        PatternRepresentation.Condition firstCondition = parseCondition(firstConditionNode);
        conditions.add(firstCondition);

        if (conditionsNode.nChildren() > 1) {
            Tree<String> concatNode = conditionsNode.child(1);
            Tree<String> remainingConditionsNode = conditionsNode.child(2);

            PatternRepresentation.Condition.Concatenator concatenator = ConcatenatorParser.parseConditionConcatenator(concatNode);
            List<PatternRepresentation.Condition> remainingConditions = parseConditions(remainingConditionsNode);

            if (!remainingConditions.isEmpty()) {
                PatternRepresentation.Condition firstRemainingCondition = remainingConditions.get(0);
                remainingConditions.set(0, new PatternRepresentation.Condition(
                        firstRemainingCondition.variable(),
                        firstRemainingCondition.operator(),
                        firstRemainingCondition.value(),
                        concatenator
                ));
            }
            conditions.addAll(remainingConditions);
        }
        return conditions;
    }
    public static PatternRepresentation.Condition parseCondition(Tree<String> conditionNode) {
        String variable = null;
        PatternRepresentation.Condition.Operator operator = null;
        Object value = null;
    
        // Extract variable name from first leaf
        variable = conditionNode.visitLeaves().get(0);
    
        for (Tree<String> child : conditionNode) {
            String content = child.content();
    
            switch (content) {
                case "<opNum>":
                case "<opStr>":
                case "<opBool>":
                    operator = OperatorParser.parseOperator(child);
                    break;
    
                case "<iNum>":
                    value = ValueParser.parseINum(child);
                    break;
    
                case "<boolean>":
                    value = ValueParser.parseBooleanValue(child);
                    break;
    
                default:
                    // Handle grammar-generated Value terminals
                    if (content.contains("StringValue")) {
                        value = ValueParser.parseStringValue(child);
                    } else if (content.contains("Value")) {
                        value = ValueParser.parseFNum(child);
                    }
            }
        }
    
        if (variable == null) {
            throw new IllegalArgumentException("Condition must have variable defined");
        } else if (operator == null) {
            throw new IllegalArgumentException("Condition must have operator defined");
        }
    
        return new PatternRepresentation.Condition(variable, operator, value, null);
    }
    
}
