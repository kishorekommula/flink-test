package io.intellisense.testproject.eng.jobs;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.DefaultRowFormatBuilder;
import org.apache.flink.api.java.tuple.Tuple2;


public class StreamingGameScore
{
    public static void main( String[] args ) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = StreamUtil.getDataStream(params, env);
        DataStream<Tuple2<String, Integer>> players =
                dataStream.map(new ExtractPlayerInfo()).filter(new FilterPlayers());
       // players.print();
        DataStream<String> playersStr = players.map(new ConvertString());
        dataStream.print();
        env.execute("Players");
    }

    public static class ExtractPlayerInfo implements MapFunction<String, Tuple2<String, Integer>>{

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
          String tokens[] = value.split(",");
          return Tuple2.of(tokens[0], Integer.parseInt(tokens[1]));
        }
    }

    public static class FilterPlayers implements FilterFunction<Tuple2<String, Integer>>{

        @Override
        public boolean filter(Tuple2<String, Integer> value) throws Exception {
            return value.f1 > 70;

        }
    }
    
    public static class ConvertString implements MapFunction<Tuple2<String, Integer>, String> {

		@Override
		public String map(Tuple2<String, Integer> value) throws Exception {
			return value.f0+"-"+value.f1;
		}
    	
    }
}
