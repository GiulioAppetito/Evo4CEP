
# EvoCo4CEP — Evolutionary Optimization for Complex Event Processing Patterns

## ✨ About

**EvoCo4CEP** is a framework for the automated discovery of Complex Event Processing (CEP) patterns in event streams using evolutionary algorithms.  
It combines **Apache Flink CEP** for real-time event processing and **JGEA** (Java Genetic Evolutionary Algorithm) for grammar-based pattern inference.

---

## 📌 Features

- **Grammar-Based Pattern Generation**: Auto-generate event matching grammars.
- **Target Sequence Labeling**: Define ground truth for supervised learning.
- **Evolutionary Pattern Optimization**: Discover event patterns using genetic programming.
- **Apache Flink CEP Integration**: Real-time scalable stream processing.
- **Dockerized Deployment**: Easy-to-run distributed architecture with Docker and Docker Compose.

---

## 📂 Repository Structure

- `src/main/` — Java source code (grammar generation, pattern inference, fitness evaluation).
- `scripts/` — Bash scripts to manage the architecture (start, stop, run experiments).
- `resources/` — Datasets, grammars, configurations.
- `docker-compose.yml` — Docker environment setup for Flink CEP and the application.
- `config.properties` — Application configuration (datasets, grammar, evaluation).
- `experiment.txt` — Evolutionary algorithm setup (EA parameters).

---

## ⚙️ Configuration

### Flink Configuration (`flink-conf.yaml`)

Basic cluster settings:
```properties
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 3072m
taskmanager.numberOfTaskSlots: 16
parallelism.default: 4
execution.checkpointing.timeout: 60000
```

---

### Application Configuration (`config.properties`)

Example settings:
```properties
datasetDirPath=/workspace/src/main/resources/datasets/sources/
csvFileName=odysseus-sshd-processed-simple.csv
grammarDirPath=/workspace/src/main/resources/grammars/generated/
grammarFileName=generatedGrammar.bnf
targetDatasetPath=/workspace/src/main/resources/datasets/target/targetDataset.csv
grammarType=BOUNDED_DURATION_AND_KEY_BY
keyByField=ip_address
targetWithinWindowSeconds=10
targetFromTimes=5
targetToTimes=10
conditionAttributes=successful_login
targetStrategy=skipToNext
individualStrategy=skipToNext
printIndividuals=true
```

---

## 🐳 Running the Architecture (with Docker)

### Prerequisites
- [Docker](https://www.docker.com/) installed
- [Docker Compose](https://docs.docker.com/compose/) installed

### Steps

**1️⃣ Start the architecture:**
```bash
./scripts/manage-architecture.sh --start
```

**2️⃣ Launch an experiment:**
```bash
./scripts/run-experiment.sh <nr> <nt>
```

**3️⃣ Stop the architecture:**
```bash
./scripts/manage-architecture.sh --stop
```

Access Flink UI: [http://localhost:8081](http://localhost:8081)

---

## 🔬 Workflow

1. **Define target sequences** using `TargetSequencesGenerator`.
2. **Generate grammars** based on dataset properties with `GrammarGenerator`.
3. **Launch evolutionary experiments** using the evolutionary configuration `experiment.txt`.
4. **Evaluate** patterns using Precision, Recall, and F1-score metrics.

---

## 🧪 Evolutionary Algorithm Settings (`experiment.txt`)

Example configuration:
```text
$nEvals = [5000]

ea.experiment(
  runs = (randomGenerator = (seed = [1:1:10]) * [m.defaultRG()]) *
    (solver = (nEval = $nEvals) * [
      ea.s.ga(
        name = "gp";
        nPop = 100;
        representation = ea.r.cfgTree(grammar = ea.grammar.fromProblem(problem = tesi.problem.patternInferenceProblem()));
        mapper = ea.m.grammarTreeBP(problem = tesi.problem.patternInferenceProblem())
      )
    ]) * [
    ea.run(problem = tesi.problem.patternInferenceProblem())
  ];
  listeners = [
    ea.l.console(),
    ea.l.bestCsv(path = "../RESULTS/{name}/{startTime}/cep-best.csv"),
    ea.l.savePlotForExp(path = "../RESULTS/{name}/{startTime}/bestFitness", plot = ea.plot.multi.quality(x=ea.f.nOfEvals()))
  ]
)
```

---

## 📚 References

- [Apache Flink](https://flink.apache.org/)
- [Apache Flink CEP Documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/libs/cep/)
- [JGEA Evolutionary Algorithm Library](https://github.com/ericmedvet/jgea)
- [Docker Documentation](https://docs.docker.com/)

---

