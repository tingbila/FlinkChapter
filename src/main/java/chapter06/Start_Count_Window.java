package chapter06;

import bean.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.MyUserBehaviorSource;

//计数窗口-滚动
public class Start_Count_Window {
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
//        WindowedStream<UserBehavior, Long, GlobalWindow> countWindowWS = keyedStream.countWindow(5);
        WindowedStream<UserBehavior, Long, GlobalWindow> countWindowWS = keyedStream.countWindow(5, 2);
        DataStream<String> sumPvData = countWindowWS.process(new ProcessWindowFunction<UserBehavior, String, Long, GlobalWindow>() {
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

        env.execute("Start_Count_Window");
    }
}
