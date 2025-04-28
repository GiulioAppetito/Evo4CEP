package representation.mappers.treerepresentationparsers;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import representation.PatternRepresentation;

public class ClauseParser {

    public static PatternRepresentation.WithinClause parseWithinClause(Tree<String> withinClauseNode) {
        float duration = ValueParser.parseFNum(withinClauseNode.child(0));
        return new PatternRepresentation.WithinClause(duration);
    }

    public static PatternRepresentation.KeyByClause parseKeyByClause(Tree<String> child) {
        String key = child.visitLeaves().get(0);
        return new PatternRepresentation.KeyByClause(key);
    }
}
