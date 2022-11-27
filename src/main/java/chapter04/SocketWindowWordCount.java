package chapter04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        // 执行环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(3);

        // 数据源
        DataStreamSource<String> socketTextStream  = env.socketTextStream("192.168.179.101", 9999, "\n");

        // 处理过程
        DataStream<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator  = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.toLowerCase().split("\\W+");
                for (String word : words) {
                    if (word.length() > 0) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(value -> value.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumData  = tuple2StringKeyedStream.sum(1);

        // sink输出
        sumData.print().disableChaining();
        
        //这一行代码一定要实现,否则程序不会执行.
        env.execute("SocketCountJava");
    }
}
