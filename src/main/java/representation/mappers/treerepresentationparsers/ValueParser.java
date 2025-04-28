package representation.mappers.treerepresentationparsers;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import utils.ColoredText;

public class ValueParser {

    public static float parseFNum(Tree<String> fNumNode) {
        StringBuilder fNumStr = new StringBuilder();
        boolean hasDigit = false;
    
        // Iterate through each child node of the <attrValue> tree
        for (Tree<String> child : fNumNode) {
            String content = child.content().trim();
    
            // Handle fixed terminals like '.' and 'E'
            if (content.equals(".") || content.equals("E")) {
                fNumStr.append(content);
            }
            // Handle grammar-generated nodes like <sign>, <digit>, <xxxExp>
            else if (content.contains("Sign") || content.contains("digit") || content.contains("Digit") || content.contains("Exp")) {
                
                String inner = child.child(0).content().trim();
                fNumStr.append(inner);
                if (inner.matches("[0-9]")) hasDigit = true;
                
            }
            // Handle fallback cases with terminal digits (e.g., "2")
            else if (content.matches("[0-9]")) {
                fNumStr.append(content);
                hasDigit = true;
            }
            // Handle fallback sign terminals (e.g., "+", "-")
            else if (content.equals("+") || content.equals("-")) {
                fNumStr.append(content);
            }
            // Invalid node type
            else {
                System.out.println(ColoredText.YELLOW + "[ValueParser] Invalid node in floating-point number: '" + content + "'" + ColoredText.RESET);
                throw new NumberFormatException("Invalid node type in floating-point number: '" + content + "'");
            }
        }
    
        // At least one digit is required
        if (!hasDigit) {
            System.out.println(ColoredText.YELLOW + "[ValueParser]: !hasDigit" + ColoredText.RESET);
            throw new NumberFormatException("No digits found — cannot parse float.");
        }
    
        String value = fNumStr.toString();
        System.out.println(ColoredText.YELLOW + "[ValueParser] Parsed fNum: '" + value + "'" + ColoredText.RESET);
    
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse float from: '" + value + "'");
        }
    }
    
    public static boolean parseBooleanValue(Tree<String> boolNode) {
        String value = boolNode.visitLeaves().get(0);
        return Boolean.parseBoolean(value);
    }

    public static String parseStringValue(Tree<String> strNode) {
        return strNode.visitLeaves().get(0);
    }

    public static int parseINum(Tree<String> iNumNode) {
        StringBuilder iNumStr = new StringBuilder();
    
        // Iterate through all leaf nodes and extract digit characters
        for (Tree<String> digitNode : iNumNode.leaves()) {
            String content = digitNode.content();
            if (content.matches("[0-9]")) { // Only allow digits
                iNumStr.append(content);
            }
        }
    
        // Trim and validate the extracted string
        String value = iNumStr.toString().trim();
        if (value.isEmpty()) {
            throw new NumberFormatException("Error: Trying to parse an empty string as an integer.");
        }
    
        return Integer.parseInt(value);
    }
    
    
}
