package problem;

import fitness.fitnesstypes.FitnessFunctionEnum;
import io.github.ericmedvet.jnb.core.Cacheable;
import io.github.ericmedvet.jnb.core.Discoverable;

@Discoverable(prefixTemplate = "tesi.problem")
public class Problems {

    // Private constructor to avoid instantiation
    private Problems() {
    }

    // Create a GrammarBasedProblem
    @SuppressWarnings("unused")
    @Cacheable
    public static PatternInferenceProblem patternInferenceProblem() throws Exception {
        String configPath = System.getenv("CONFIG_PATH");
        FitnessFunctionEnum fitnessFunction = FitnessFunctionEnum.FBETA;

        return new PatternInferenceProblem(configPath, fitnessFunction);
    }

    @SuppressWarnings("unused")
    @Cacheable
    public static AlternativeProblem alternativeProblem() throws Exception {
        String configPath = System.getenv("CONFIG_PATH");
        FitnessFunctionEnum fitnessFunction = FitnessFunctionEnum.FBETA;

        return new AlternativeProblem(configPath, fitnessFunction);
    }


}