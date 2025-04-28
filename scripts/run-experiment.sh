#!/bin/bash

# Check parameters
if [ $# -lt 3 ]; then
    echo "Usage: $0 <nr> <nt> <type>"
    echo "nr: Number of runs, nt: Number of threads, type: 'so' (Single Objective) or 'mo' (Multi Objective)"
    exit 1
fi

# Parameters
NR=$1
NT=$2
TYPE=$3

# Validate type
if [[ "$TYPE" != "so" && "$TYPE" != "mo" ]]; then
    echo "Error: The third parameter must be 'so' (Single Objective) or 'mo' (Multi Objective)"
    exit 1
fi

# Generate timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
FILENAME="closure_times/${TIMESTAMP}_closureTime.txt"

# Create directory if not exists
mkdir -p closure_times

# Create file with initial timestamp
echo "Experiment started at: $(date)" > "$FILENAME"

echo "🚀 Starting up experiment with nr=$NR, nt=$NT, type=$TYPE..."

# Select the correct experiment file based on type
if [[ "$TYPE" == "mo" ]]; then
    EXPERIMENT_FILE="/workspace/src/main/resources/experiments/experiment.txt"
else
    EXPERIMENT_FILE="/workspace/src/main/resources/experiments/experimentSO.txt"
fi

# Run the Docker command
docker exec flink-app java --add-opens java.base/java.util=ALL-UNNAMED \
  -jar /workspace/target/flinkCEP-Patterns-0.1-jar-with-dependencies.jar \
  -v -nr "$NR" -nt "$NT" -f "$EXPERIMENT_FILE"

# Write closure timestamp
echo "Experiment ended at: $(date)" >> "$FILENAME"

echo "✅ Experiment succeeded!"
