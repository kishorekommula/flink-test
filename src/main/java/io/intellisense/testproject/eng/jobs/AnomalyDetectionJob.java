package io.intellisense.testproject.eng.jobs;

import lombok.extern.slf4j.Slf4j;
import io.intellisense.testproject.eng.datasource.CsvDatasource;
import io.intellisense.testproject.eng.sink.influxdb.InfluxDBSink;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import java.io.InputStream;
import java.time.Instant;

@Slf4j
public class AnomalyDetectionJob {

    public static void main(String[] args) throws Exception {

        // Pass configFile location as a program argument:
        // --configFile config.local.yaml
        final ParameterTool programArgs = ParameterTool.fromArgs(args);
        final String configFile = programArgs.getRequired("configFile");
        final InputStream resourceStream = AnomalyDetectionJob.class.getClassLoader().getResourceAsStream(configFile);
        final ParameterTool configProperties = ParameterTool.fromPropertiesFile(resourceStream);
        System.out.println("configFileStream:::"+configProperties.get("influxdb.dbName"));
        // Stream execution environment
        // ...you can add here whatever you consider necessary for robustness
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(configProperties.getInt("flink.parallelism", 1));
        env.getConfig().setGlobalJobParameters(configProperties);

        // Simple CSV-table datasource
        final String dataset = programArgs.get("sensorData", "sensor-data.csv");
        final CsvTableSource csvDataSource = CsvDatasource.of(dataset).getCsvSource();
        final WatermarkStrategy<Row> watermarkStrategy = WatermarkStrategy.<Row>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> timestampExtract(event));
        final DataStream<Row> sourceStream = csvDataSource.getDataStream(env)
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("datasource-operator");
        //sourceStream.print();
        
        // TODO: wire here your datastream ETL
        // final DataStream<?> filteredStream = sourceStream.filter(...).name("data-wrangling-operator");
        // final DataStream<?> mappedStream = filteredStream.map(...).name("mapping-operator");
        // final DataStream<?> processedStream = mappedStream.process(...).name("processing-operator");

        // Sink
        final SinkFunction<?> influxDBSink = new InfluxDBSink<>(configProperties);
        // processedStream.addSink(influxDBSink).name("sink-operator");

        final JobExecutionResult jobResult = env.execute("Anomaly Detection Job");
        log.info(jobResult.toString());
    }

    private static long timestampExtract(Row event) {
        final String timestampField = (String) event.getField(0);
        return Instant.parse(timestampField).toEpochMilli();
    }
}
