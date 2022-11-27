package chapter05;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

// fromCollection(Collection) - Creates a data stream from the Java Java.util.Collection. All elements in the collection must be of the same type.
public class Source_Collection {
    public static void main(String[] args) throws Exception {
        // 0.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.Source:读取数据
        DataStreamSource<Long> sensorDS = env.fromCollection(Arrays.asList(15321312412L,15321763412L,15369732412L));

        // 2.打印
        sensorDS.print();

        // 3.执行
        env.execute();
    }
}
