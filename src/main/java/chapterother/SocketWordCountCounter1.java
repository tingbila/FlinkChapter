package chapterother;

import chapter01.WordCountStreaming;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


//Flink流处理案例开发:counter计数器
public class SocketWordCountCounter1 {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration conf = new Configuration();
//        conf.set(RestOptions.PORT, 8083);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(3);
//      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("192.168.40.101", 9999, "\n");

        DataStream<String> flatMapDataStream = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                // normalize and split the line
                String[] tokens = value.toLowerCase().split("\\W+");
                // emit the pairs
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(token);
                    }
                }
            }
        });

        DataStream<Tuple2<String, Integer>> mapDataStream = flatMapDataStream.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            private transient Counter counter;
            @Override
            public void open(Configuration parameters) throws Exception {
                MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
                MetricGroup myGroup = metricGroup.addGroup("MyGroup");  //counter组
                counter = myGroup.counter("MapCounter");//counter组当中的counter
            }

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                counter.inc(1L);
                return new Tuple2<>(value, 1);
            }
        });

        DataStream<Tuple2<String, Integer>> sumCounts = mapDataStream.keyBy(value -> value.f0).sum(1);
        sumCounts.print();


        env.execute("SocketWordCountCounter1");
    }
}
