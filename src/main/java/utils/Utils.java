package utils;

import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;

import java.io.FileInputStream;
import java.util.Properties;

public class Utils {
    public static Properties loadConfig(String filePath) throws Exception {
        Properties config = new Properties();
        try (FileInputStream input = new FileInputStream(filePath)) {
            config.load(input);
        }
        return config;
    }

    public static String getRequiredProperty(Properties config, String propertyName) {
        String value = config.getProperty(propertyName);
        if (value == null) {
            throw new IllegalArgumentException("Missing required configuration property: " + propertyName);
        }
        return value;
    }

    public static StringGrammar<String> loadGrammar(String filePath) throws Exception {
        try (FileInputStream grammarStream = new FileInputStream(filePath)) {
            return StringGrammar.load(grammarStream);
        }
    }
}
