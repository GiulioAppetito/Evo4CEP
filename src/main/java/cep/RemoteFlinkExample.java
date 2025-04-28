package cep;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RemoteFlinkExample {
    public static void main(String[] args) throws Exception {
        // Imposta l'ambiente di esecuzione remoto
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                "129.16.20.158",
                8081,
                "C:\\Users\\giuli\\IdeaProjects\\cep-genetic-algorithms-thesis-dev3\\Flink-cep-examples-main\\target\\flinkCEP-Patterns-0.1-jar-with-dependencies.jar"
        );

        // Crea un semplice data stream e stampa i risultati
        env.fromElements(1, 2, 3, 4, 5)
                .map(value -> value * 10)
                .print();

        // Esegui il job
        env.execute("Simple Flink Job");
    }
}

