package events.factory;

import events.BaseEvent;
import events.GenericEvent;
import grammar.utils.CSVTypesExtractor;
import grammar.datatypes.DataTypesEnum;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.FileReader;
import java.io.Reader;
import java.util.*;

public class CsvStreamingSourceRealTime implements SourceFunction<BaseEvent> {
    private final String csvFilePath;
    private final double speedupFactor; // 1.0 = reale, 2.0 = 2x più veloce, 0.5 = più lento
    private volatile boolean running = true;

    public CsvStreamingSourceRealTime(String csvFilePath, double speedupFactor) {
        this.csvFilePath = csvFilePath;
        this.speedupFactor = speedupFactor;
    }

    @Override
    public void run(SourceContext<BaseEvent> ctx) throws Exception {
        Set<String> allowedAttributes = new HashSet<>();
        Map<String, DataTypesEnum> columnTypes =
                CSVTypesExtractor.getColumnTypesFromCSV(csvFilePath, allowedAttributes);

        try (Reader reader = new FileReader(csvFilePath);
             CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            Iterator<CSVRecord> iterator = parser.iterator();
            long prevRawTimestamp = -1;

            while (running && iterator.hasNext()) {
                CSVRecord record = iterator.next();
                try {
                    long rawTimestamp = Long.parseLong(record.get("timestamp")); // nanosecondi
                    long timestamp = rawTimestamp / 1_000_000; // millisecondi per Flink
                    GenericEvent event = new GenericEvent(timestamp);

                    for (Map.Entry<String, DataTypesEnum> entry : columnTypes.entrySet()) {
                        String col = entry.getKey();
                        if ("timestamp".equals(col)) continue;
                        String val = record.get(col);
                        switch (entry.getValue()) {
                            case INT -> event.setAttribute(col, Integer.parseInt(val));
                            case LONG -> event.setAttribute(col, Long.parseLong(val));
                            case DOUBLE -> event.setAttribute(col, Double.parseDouble(val));
                            case BOOLEAN -> event.setAttribute(col, Boolean.parseBoolean(val));
                            case STRING -> event.setAttribute(col, val);
                        }
                    }

                    // Calcola il delay basato su timestamp nano, ma convertito in millis
                    if (prevRawTimestamp > 0) {
                        long delayNanos = rawTimestamp - prevRawTimestamp;
                        long delayMillis = delayNanos / 1_000_000;
                        long adjustedDelay = (long) (delayMillis / speedupFactor);
                        Thread.sleep(Math.min(adjustedDelay, 100)); // safety cap
                    }

                    prevRawTimestamp = rawTimestamp;

                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collectWithTimestamp(event, timestamp);
                    }

                } catch (Exception e) {
                    System.err.println("[CSV ERROR] " + record + " -> " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
