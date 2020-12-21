package io.intellisense.testproject.eng.jobs;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Cars {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> rawCarStream = StreamUtil.getDataStream(params, env);
		if(rawCarStream == null) {
			System.out.println("Data Stream is null !! so exiting");
			System.exit(1);
			return ;
		}
		DataStream<String> filteredCarStream = rawCarStream.filter(new FilterFunction<String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(String value) throws Exception {
				if(value.contains("make,modle,year,price")) {
					return false;
				}
				return true;
			}
		});
		
		DataStream<Tuple4<String, String, Integer, Integer>> carStream = 
				filteredCarStream.map(new MapCarsFunction());
		
		KeyedStream<Tuple4<String, String, Integer, Integer>, String> keyedStream= carStream.keyBy(value -> value.f0+value.f1);
		
		keyedStream.minBy(2).project(0,1,2,3).print();
		// keyedStream.print();
		env.execute("Cars Streaming");
}
	public static class MapCarsFunction implements MapFunction<String, Tuple4<String, String, Integer, Integer>>{

		@Override
		public Tuple4<String, String, Integer, Integer> map(String value) throws Exception {
			String[] carProps= value.split(",");
			return  Tuple4.of(carProps[0], carProps[1], Integer.parseInt(carProps[2]), Integer.parseInt(carProps[3]));
		}

		
	}
}
