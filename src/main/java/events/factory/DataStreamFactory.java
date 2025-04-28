package events.factory;

import events.BaseEvent;
import events.GenericEvent;
import grammar.utils.CSVTypesExtractor;
import grammar.datatypes.DataTypesEnum;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileReader;
import java.io.Reader;
import java.util.*;

public class DataStreamFactory {

    public static DataStream<BaseEvent> createDataStream(StreamExecutionEnvironment env, String csvFilePath) {
        env.getConfig().setAutoWatermarkInterval(500);
    
        // Simula ritmi reali basati su timestamp CSV
        return env
            .addSource(new CsvStreamingSourceRealTime(csvFilePath, 1000000000.0)) // 10x più veloce
            .assignTimestampsAndWatermarks(new LoggingWatermarkStrategy());
    }
    // Classe per loggare i Watermark generati
    private static class LoggingWatermarkStrategy implements WatermarkStrategy<BaseEvent> {
        private static final Logger LOG = LoggerFactory.getLogger(LoggingWatermarkStrategy.class);
        @Override
        public WatermarkGenerator<BaseEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<BaseEvent>() {
                private long maxTimestampSeen = Long.MIN_VALUE;

                @Override
                public void onEvent(BaseEvent event, long eventTimestamp, WatermarkOutput output) {
                    maxTimestampSeen = Math.max(maxTimestampSeen, eventTimestamp);
                    output.emitWatermark(new Watermark(maxTimestampSeen));

                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    output.emitWatermark(new Watermark(maxTimestampSeen));
                    
                }
            };
        }

        @Override
        public TimestampAssigner<BaseEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> event.getTimestamp();
        }
    }
}
