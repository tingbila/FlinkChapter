package chapter06;

import bean.MinMaxTemp;
import bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;


//展示了如何使用ReduceFunction和ProcessWindowFunction的组合来实现每个传感器
//每5秒发出一次最高和最低温度以及窗口的结束时间戳。
//使用ReduceFunction执行增量聚合，使用ProcessWindowFunction计算最终结果。
public class IncrementalAndProcessWindowFunctionCase {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("heartbeat.timeout", "18000000");

        // 获取执行环境并设置配置
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> dataStreamSource = env.socketTextStream("192.168.40.101", 9999, "\n");

        DataStream<Tuple3<String, Integer, Integer>> sensorDataStream = dataStreamSource.map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(String line) throws Exception {
                String[] spited = line.split(",");
                //我们要提前想好这个数据类型,因为下面是reduce函数要用
                return Tuple3.of(spited[0], Integer.valueOf(spited[2]), Integer.valueOf(spited[2]));
            }
        });

        KeyedStream<Tuple3<String, Integer, Integer>, String> keyedStream = sensorDataStream.keyBy(value -> value.f0);
        WindowedStream<Tuple3<String, Integer, Integer>, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(10));

        DataStream<MinMaxTemp> processDS = windowedStream.reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2) throws Exception {
                //增量计算最低和最高温度
                return Tuple3.of(value1.f0, Math.min(value1.f1, value2.f1), Math.max(value1.f2, value2.f2));
            }
        }, new ProcessWindowFunction<Tuple3<String, Integer, Integer>, MinMaxTemp, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Tuple3<String, Integer, Integer>> elements, Collector<MinMaxTemp> out) throws Exception {
                //在ProcessWindowFunction中直接获取增量计算的最终结果
                Tuple3<String, Integer, Integer> minMax = elements.iterator().next();
                long windowStart = context.window().getStart();
                long windowEnd = context.window().getEnd();
                out.collect(new MinMaxTemp(key, minMax.f1, minMax.f2, windowStart, windowEnd));
            }
        });

        processDS.print();

        env.execute("IncrementalAndProcessWindowFunctionCase");
    }
}