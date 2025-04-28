package grammar.utils;

import utils.CsvAnalyzer;
import grammar.datatypes.DataTypesEnum;
import grammar.datatypes.GrammarTypes;

import java.util.*;
import static utils.Utils.loadConfig;

public class GrammarBuilder {

    public static String buildGrammar(Map<String, DataTypesEnum> columnTypes,
                                      Map<String, Set<String>> uniqueStringValues,
                                      List<DataTypesEnum> uniqueColumnTypes,
                                      GrammarTypes grammarType,
                                      String csvFilePath,
                                      long numEvents) throws Exception {

        StringBuilder grammar = new StringBuilder();
        Properties myConfig = loadConfig(System.getenv("CONFIG_PATH"));

        // Load min/max ranges for numeric attributes
        Map<String, Float[]> numericRanges = CsvAnalyzer.extractNumericMinMax(csvFilePath, columnTypes);

        // Allowed attributes (for <condition>)
        Set<String> allowedAttributes = new HashSet<>();
        String conditionAttributesConfig = myConfig.getProperty("conditionAttributes", "").trim();
        if (!conditionAttributesConfig.isEmpty()) {
            allowedAttributes.addAll(Arrays.asList(conditionAttributesConfig.split(",")));
        }

        // Duration-based settings
        final long maxBoundedValue = CsvAnalyzer.calculateDurationFromCsv(csvFilePath);
        final int maxDigits = String.valueOf(maxBoundedValue).length();
        final int maxTimesDigit = String.valueOf(numEvents).length();

        // Top-level pattern structure
        grammar.append("<pattern> ::= <events> <key_by> <withinClause> | <events> <withinClause> | <events> <key_by> | <events>\n");
        grammar.append("<events> ::= <event> | <event> <eConcat> <events>\n");
        grammar.append("<event> ::= <identifier> <conditions> <quantifier>\n");
        grammar.append("<conditions> ::= <condition> | <condition> <cConcat> <conditions>\n");
        grammar.append("<cConcat> ::= and | or\n");
        grammar.append("<identifier> ::= event\n");
        grammar.append("<eConcat> ::= next | followedBy | followedByAny\n");

        // Conditions per attribute
        grammar.append("<condition> ::= ");
        StringJoiner conditionJoiner = new StringJoiner(" | ");
        for (Map.Entry<String, DataTypesEnum> entry : columnTypes.entrySet()) {
            String column = entry.getKey();
            if (!column.equals("timestamp") && (allowedAttributes.isEmpty() || allowedAttributes.contains(column))) {
                conditionJoiner.add(generateConditionForType(column, entry.getValue(), uniqueStringValues));
            }
        }
        grammar.append(conditionJoiner).append("\n");

        // Key_by
        if (grammarType == GrammarTypes.BOUNDED_KEY_BY || grammarType == GrammarTypes.BOUNDED_DURATION_AND_KEY_BY) {
            String keyByField = myConfig.getProperty("keyByField");
            grammar.append("<key_by> ::= ").append(keyByField).append("\n");
        } else {
            grammar.append("<key_by> ::= ");
            StringJoiner keyByJoiner = new StringJoiner(" | ");
            for (String column : columnTypes.keySet()) {
                if (!column.equals("timestamp")) {
                    keyByJoiner.add(column);
                }
            }
            grammar.append(keyByJoiner).append("\n");
        }

        // Within 
        // Define within clause (time-bound constraint)
        if (grammarType.equals(GrammarTypes.BOUNDED_DURATION) || grammarType.equals(GrammarTypes.BOUNDED_DURATION_AND_KEY_BY)) {
            System.out.println("[GrammarBuilder] maxBoundedValue : " + maxBoundedValue + ".");

            grammar.append("<withinClause> ::= <boundedDuration>\n");
            grammar.append("<boundedDuration> ::= ");

            for (int i = 1; i <= maxDigits; i++) {
                grammar.append("<digit>");
                for (int j = 1; j < i; j++) {
                    grammar.append(" <digit>");
                }
                if (i < maxDigits) {
                    grammar.append(" | ");
                }
            }
            grammar.append("\n");
        } if (grammarType.equals(GrammarTypes.UNBOUNDED)) {
            System.out.println("[GrammarBuilder] Unbounded duration.");
            grammar.append("<withinClause> ::= <iNum>\n");
        }

        // Operators
        if (uniqueColumnTypes.stream().anyMatch(t -> t == DataTypesEnum.INT || t == DataTypesEnum.LONG || t == DataTypesEnum.DOUBLE)) {
            grammar.append("<opNum> ::= equal | lt | gt\n");
        }
        if (uniqueColumnTypes.contains(DataTypesEnum.BOOLEAN)) {
            grammar.append("<opBool> ::= equal\n");
        }
        if (uniqueColumnTypes.contains(DataTypesEnum.STRING)) {
            grammar.append("<opStr> ::= equal\n");
        }

        // Quantifiers
        if (grammarType == GrammarTypes.BOUNDED_DURATION || grammarType == GrammarTypes.BOUNDED_DURATION_AND_KEY_BY) {
            grammar.append("<quantifier> ::= oneOrMore | optional | times <boundedTimes> | range <boundedTimes> <boundedTimes>\n");
            grammar.append("<boundedTimes> ::= ");
            for (int i = 1; i <= maxTimesDigit; i++) {
                grammar.append("<greaterThanZeroDigit>");
                for (int j = 1; j < i; j++) grammar.append(" <digit>");
                if (i < maxTimesDigit) grammar.append(" | ");
            }
            grammar.append("\n");
        } else {
            grammar.append("<quantifier> ::= oneOrMore | optional | times <greaterThanZeroNum> | range <greaterThanZeroNum> <greaterThanZeroNum>\n");
        }

        // Digits
        grammar.append("<digit> ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9\n");
        grammar.append("<greaterThanZeroDigit> ::= 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9\n");
        grammar.append("<iNum> ::= <digit> | <iNum> <digit>\n");
        grammar.append("<greaterThanZeroNum> ::= <greaterThanZeroDigit> | <greaterThanZeroNum> <digit> | <greaterThanZeroDigit> <greaterThanZeroNum>\n");

        // Boolean
        if (columnTypes.containsValue(DataTypesEnum.BOOLEAN)) {
            grammar.append("<boolean> ::= true | false\n");
        }

        // Numeric value rules (per attribute)
        for (Map.Entry<String, Float[]> entry : numericRanges.entrySet()) {
            String column = entry.getKey();
            Float min = entry.getValue()[0];
            Float max = entry.getValue()[1];

            float minAbs = Math.min(Math.abs(min), Math.abs(max));
            float maxAbs = Math.max(Math.abs(min), Math.abs(max));

            int minExp = (minAbs > 0) ? (int) Math.floor(Math.log10(minAbs)) : 0;
            int maxExp = (maxAbs > 0) ? (int) Math.floor(Math.log10(maxAbs)) : 0;

            grammar.append("<").append(column).append("Value> ::= <").append(column).append("Sign> <digit> . <digit> <digit> E <").append(column).append("Exp>\n");

            grammar.append("<").append(column).append("Sign> ::= ");
            if (min >= 0 && max >= 0) {
                grammar.append("+\n");
            } else if (max <= 0 && min <= 0) {
                grammar.append("-\n");
            } else {
                grammar.append("+ | -\n");
            }

            grammar.append("<").append(column).append("Exp> ::= ");
            StringJoiner expJoiner = new StringJoiner(" | ");
            for (int i = minExp; i <= maxExp; i++) {
                expJoiner.add(String.valueOf(i));
            }
            grammar.append(expJoiner).append("\n");
        }

        // String value rules (per attribute)
        for (Map.Entry<String, Set<String>> entry : uniqueStringValues.entrySet()) {
            grammar.append("<").append(entry.getKey()).append("StringValue> ::= ");
            StringJoiner stringValuesJoiner = new StringJoiner(" | ");
            for (String value : entry.getValue()) {
                stringValuesJoiner.add("'" + value + "'");
            }
            grammar.append(stringValuesJoiner).append("\n");
        }

        return grammar.toString();
    }

    private static String generateConditionForType(String column, DataTypesEnum type, Map<String, Set<String>> uniqueStringValues) {
        return switch (type) {
            case INT, DOUBLE, LONG -> String.format("%s <opNum> <%sValue>", column, column);
            case BOOLEAN -> String.format("%s <opBool> <boolean>", column);
            case STRING -> String.format("%s <opStr> <%sStringValue>", column, column);
            default -> throw new IllegalArgumentException("Unsupported data type: " + type);
        };
    }
}
