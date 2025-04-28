package grammar.utils;

import java.io.FileWriter;
import java.io.IOException;

public class GrammarFileWriter {

    public static void writeToFile(String content, String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(content);
            System.out.println("Grammar generated and saved to " + filePath);
        } catch (IOException e) {
            System.err.println("An error occurred while writing to the file: " + e.getMessage());
        }
    }
}
