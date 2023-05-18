package chapter06;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class Window_IncreAggr {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("heartbeat.timeout", "18000000");

        // 获取执行环境并设置配置
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("192.168.40.101", 9999, "\n");

        // parse the data, group it, window it, and aggregate the counts
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // normalize and split the line
                String[] tokens = value.toLowerCase().split("\\W+");

                // emit the pairs
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }).keyBy(value -> value.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> dataWS = keyedStream.timeWindow(Time.seconds(10));

        DataStream<Integer> aggregateDs = dataWS.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
            //初始化  => 初始值 0
            //注意:这个不是open方法,不是只初始化一次,应该是每次窗口重新生成的时候都会重新生成一个Accumulator
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            //累加操作 => 每来一条数据，如何进行累加
            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                System.out.println("add...");
                return accumulator + 1;
            }

            //获取累加器结果
            @Override
            public Integer getResult(Integer accumulator) {
                System.out.println("get result...");
                return accumulator;
            }

            //合并多个累加器的结果，会话窗口才会调用
            //debug调试的时候确实不会执行这里
            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        });

        aggregateDs.print("------------->聚合结果:----------->");

        env.execute("Window_IncreAggr");
    }
}
