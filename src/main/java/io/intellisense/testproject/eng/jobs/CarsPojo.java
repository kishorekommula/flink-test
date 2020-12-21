package io.intellisense.testproject.eng.jobs;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CarsPojo {
	public static class Car{
		public String make;
		public String model;
		public String year;
		public String price;
		
		public Car() {
			
		}
		
		@Override
		public String toString() {
			return make+"-"+model+"-"+year+"-"+price;
		}
	}

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
		
		DataStream<Car> carStream = 
				filteredCarStream.map(new MapCarsFunction());
		
		KeyedStream<Car, String> keyedStream= carStream.keyBy(car -> car.make+car.model);
		
		DataStream<Car> minYearStream = keyedStream.minBy("year");
		minYearStream.print();
		env.execute("Cars Streaming");
}
	public static class MapCarsFunction implements MapFunction<String, Car>{

		@Override
		public Car map(String value) throws Exception {
			String[] carProps= value.split(",");
			Car car = new Car();
			car.make=carProps[0];
			car.model=carProps[1];
			car.year=carProps[2];
			car.price=carProps[3];
			return  car;
		}

		
	}
}
