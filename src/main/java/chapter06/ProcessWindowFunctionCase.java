package chapter06;

import bean.MinMaxTemp;
import bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;


//在系统内部由ProcessWindowFunction 处理的窗口会将所有已分配的事件存储在 ListState 中,通过将所有事件收集起来且提供对于窗口元数据及
//其他一些特性的访问和使用，ProcesswindowFunction的应用场景比Reduce-Function和 AggregateFunction更加广泛。但和执行增量聚合的窗口相比，收集全部事件的窗口其状态要大得多。
public class ProcessWindowFunctionCase {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("heartbeat.timeout", "18000000");

        // 获取执行环境并设置配置
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> dataStreamSource = env.socketTextStream("192.168.40.101", 9999, "\n");
        DataStream<WaterSensor> sensorDataStream = dataStreamSource.map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String line) throws Exception {
                String[] spited = line.split(",");
                return new WaterSensor(spited[0], Long.valueOf(spited[1]), Integer.valueOf(spited[2]));
            }
        });


        KeyedStream<WaterSensor, String> keyedStream = sensorDataStream.keyBy(value -> value.getId());

        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(10));
        SingleOutputStreamOperator<MinMaxTemp> processDS = windowedStream.process(new ProcessWindowFunction<WaterSensor, MinMaxTemp, String, TimeWindow>() {
            private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            //该ProcessWindowFunction用于计算每个窗口内的最低和最高温度读数,它会将读数连同窗口开始、结束时间戳一起发出。
            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<MinMaxTemp> out) throws Exception {
                double min = Double.MAX_VALUE;
                double max = Double.MIN_VALUE;

                for (WaterSensor sensor : elements) {
                    double temperature = sensor.getVc();
                    if (temperature < min) {
                        min = temperature;
                    }
                    if (temperature > max) {
                        max = temperature;
                    }
                }
                long windowStart = context.window().getStart();
                long windowEnd = context.window().getEnd();

                out.collect(new MinMaxTemp(key, min, max, windowStart, windowEnd));
            }
        });

        processDS.print();

        env.execute("ProcessWindowFunctionCase");
    }
}

