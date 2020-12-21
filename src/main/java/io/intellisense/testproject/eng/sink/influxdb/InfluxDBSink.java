package io.intellisense.testproject.eng.sink.influxdb;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import io.intellisense.testproject.eng.model.DataPoint;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

@Slf4j
@RequiredArgsConstructor
public class InfluxDBSink<T extends DataPoint> extends RichSinkFunction<T> {

    transient InfluxDB influxDB;

    final ParameterTool configProperties;

    // cluster metrics
    final Accumulator recordsIn = new IntCounter(0);
    final Accumulator recordsOut = new IntCounter(0);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator(getClass().getSimpleName() + "-recordsIn", recordsIn);
        getRuntimeContext().addAccumulator(getClass().getSimpleName() + "-recordsOut", recordsOut);
        influxDB = InfluxDBFactory.connect(
                configProperties.getRequired("influxdb.url"),
                configProperties.getRequired("influxdb.username"),
                configProperties.getRequired("influxdb.password"));
        final String dbname = configProperties.getRequired("influxdb.dbName");
        influxDB.setDatabase(dbname);
        influxDB.enableBatch(BatchOptions.DEFAULTS);
    }

    @Override
    public void invoke(T dataPoint, Context context) {
        recordsIn.add(1);
        try {
            // Map model entity to InfluxDB Point via InfluxDB annotations
            final Point point = Point.measurementByPOJO(DataPoint.class)
                    .tag("sensor", dataPoint.getSensor())
                    .addFieldsFromPOJO(dataPoint)
                    .build();
            influxDB.write(point);
            recordsOut.add(1);
        } catch(Exception e) {
            log.error(e.getMessage());
        }
    }
}