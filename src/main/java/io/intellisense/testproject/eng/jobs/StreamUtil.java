package io.intellisense.testproject.eng.jobs;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamUtil {
	public static DataStream<String> getDataStream(ParameterTool params, StreamExecutionEnvironment env) {
		DataStream<String> dataStream;
		if(params.has("input")) {
			System.out.println("Getting the data Input:::::");
			dataStream = env.socketTextStream("localhost", 9999);
		} else {
			String filePath = "file:///Users/kishorkommula/test-project/testproject.eng.backend-master/src/main/resources/cars.csv";
			System.out.println("filePath::"+filePath);
			dataStream = env.readTextFile(filePath);

		}
		
		return dataStream;
	}

}
