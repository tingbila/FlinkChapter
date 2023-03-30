package chapter06;

import bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.MyUserBehaviorSource;

public class Start_Session_Window {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 读取数据
        DataStreamSource<UserBehavior> dataStreamSource = env.addSource(new MyUserBehaviorSource());
        DataStream<UserBehavior> filterDataStream = dataStreamSource.filter(value -> "pv".equals(value.getBehavior()));
        filterDataStream.print();

        // 通过keyBy(new KeySelector {...})指定字段进行分区或者通过lambda表达式进行分区
        KeyedStream<UserBehavior, Long> keyedStream = filterDataStream.keyBy(value -> value.getUserId());
        WindowedStream<UserBehavior, Long, TimeWindow> dataWS = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        DataStream<String> sumPvData = dataWS.process(new ProcessWindowFunction<UserBehavior, String, Long, TimeWindow>() {
            // 定义一个变量，来统计条数
            private Long pvCount = 0L;

            @Override
            public void process(Long key, Context context, Iterable<UserBehavior> elements, Collector<String> out) throws Exception {
                for (UserBehavior ele : elements) {
                    pvCount += 1;
                }
                out.collect("用户" + String.valueOf(key) + " 的pv是: " + String.valueOf(pvCount));
                pvCount = 0L;
            }
        });

        sumPvData.print();

        env.execute("Start_Session_Window");
    }
}
