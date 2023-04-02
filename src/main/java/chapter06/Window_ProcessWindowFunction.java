package chapter06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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

public class Window_ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

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
        DataStream<String> processDS = dataWS.process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
            private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                System.out.println("--------------->全量触发计算-------------->");
                String windowStart = sdf.format(context.window().getStart());
                String windowEnd = sdf.format(context.window().getEnd());
                out.collect("Key:" + key + " 窗口开始时间:" + windowStart + " 窗口结束时间:" + windowEnd + "  单词统计次数: " + String.valueOf(elements.spliterator().estimateSize()));
            }
        });

        processDS.print();

        env.execute("Window_ProcessWindowFunction");
    }
}
