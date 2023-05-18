package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// 展示了如何使用 AggregateFunction 计算每个窗口内传感器读数的平均温度。 其累加器负责维护不断变化的温度总和及数量，
// getResult(）方法用来计算平均值。
// 对于键值窗口操作 (keyed windows)，窗口函数 (window function) 是和每个键 (key) 单独绑定的。具体来说，窗口函数将被应用于每个键对应的窗口中的元素。
// 这些窗口函数将以每个键为单位独立运行，并且对于每个键的每个窗口，会调用窗口函数来处理该窗口中的元素。每个窗口函数都可以访问窗口中的所有元素，并输出计算结果。
public class WindowIncreAggrCase2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("heartbeat.timeout", "18000000");

        // 获取执行环境并设置配置
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> dataStreamSource = env.socketTextStream("192.168.40.101", 9999, "\n");
        DataStream<WaterSensor> mapDataStream = dataStreamSource.map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String line) throws Exception {
                String[] spited = line.split(",");
                return new WaterSensor(spited[0], Long.valueOf(spited[1]), Integer.valueOf(spited[2]));
            }
        });

        mapDataStream.print();

        KeyedStream<WaterSensor, String> keyedStream = mapDataStream.keyBy(value -> value.getId());  //以传感器id为键值进行分区
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(15));

        // 用于计算每个传感器平均温度的AggregateFunction
        // 累加器用于保存温度总和及事件数量
        SingleOutputStreamOperator<Tuple2<String, Double>> aggregateDs = windowedStream.aggregate(new AggregateFunction<WaterSensor, Tuple3<String, Integer, Double>, Tuple2<String, Double>>() {
            @Override
            public Tuple3<String, Integer, Double> createAccumulator() {
                return Tuple3.of("", 0, 0.0);
            }

            @Override
            public Tuple3<String, Integer, Double> add(WaterSensor value, Tuple3<String, Integer, Double> accumulator) {
                return Tuple3.of(value.getId(), value.getVc() + accumulator.f1, accumulator.f2 + 1);
            }

            @Override
            public Tuple2<String, Double> getResult(Tuple3<String, Integer, Double> accumulator) {
                return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
            }

            @Override
            public Tuple3<String, Integer, Double> merge(Tuple3<String, Integer, Double> a, Tuple3<String, Integer, Double> b) {
                return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
            }
        });

        aggregateDs.print("------------->聚合结果:----------->");

        env.execute("WindowIncreAggrCase2");
    }
}
