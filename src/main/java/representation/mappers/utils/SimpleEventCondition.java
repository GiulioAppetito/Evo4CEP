package representation.mappers.utils;

import events.BaseEvent;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import representation.PatternRepresentation;

import java.util.Map;

public class SimpleEventCondition<E extends BaseEvent> extends SimpleCondition<E> {
    private final PatternRepresentation.Condition condition;

    public SimpleEventCondition(PatternRepresentation.Condition condition) {
        this.condition = condition;
    }

    @Override
    public boolean filter(E value) throws Exception {
        Map<String, Object> eventMap = value.toMap();
        Object fieldValue = eventMap.get(condition.variable());

        // Check if the field value matches the expected type and apply condition
        if (fieldValue == null) {
            System.out.println("Variable not found: " + condition.variable());
            return false;
        }

        switch (condition.operator()) {
            case EQUAL:
                return fieldValue.equals(condition.value());

            case NOT_EQUAL:
                return !fieldValue.equals(condition.value());

            case LESS_THAN:
                if (fieldValue instanceof Number && condition.value() instanceof Number) {
                    return ((Number) fieldValue).doubleValue() < ((Number) condition.value()).doubleValue();
                }
                break;

            case GREATER_THAN:
                if (fieldValue instanceof Number && condition.value() instanceof Number) {
                    return ((Number) fieldValue).doubleValue() > ((Number) condition.value()).doubleValue();
                }
                break;

            default:
                throw new IllegalArgumentException("Unknown operator: " + condition.operator());
        }

        System.out.println("Unsupported type or operator for value comparison: " + fieldValue);
        return false;
    }
}