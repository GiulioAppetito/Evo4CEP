package representation.mappers;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import representation.PatternRepresentation;
import representation.mappers.treerepresentationparsers.ClauseParser;
import representation.mappers.treerepresentationparsers.EventParser;

import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;

public class TreeToRepresentationMapper implements Function<Tree<String>, PatternRepresentation> {

    @Override
    public PatternRepresentation apply(Tree<String> tree) {
        List<PatternRepresentation.Event> events = new ArrayList<>();
        PatternRepresentation.WithinClause withinClause = null;
        PatternRepresentation.KeyByClause keyByClause = null;

        for (Tree<String> child : tree) {
            if ("<events>".equals(child.content())) {
                events = EventParser.parseEvents(child);
            } else if ("<withinClause>".equals(child.content())) {
                withinClause = ClauseParser.parseWithinClause(child);
            } else if ("<key_by>".equals(child.content())) {
                keyByClause = ClauseParser.parseKeyByClause(child);
            }
        }

        PatternRepresentation patternRepresentation = new PatternRepresentation(events, withinClause, keyByClause);
        return patternRepresentation;
    }
}
