package chapter06;

import bean.UserBehavior;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.MyUserBehaviorSource;

import java.util.List;

//初始化体验窗口
//需求:基于用户的浏览信息统计用户的浏览pv（每5秒钟统计一次）
public class Start_Time_Window {
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
//        WindowedStream<UserBehavior, Long, TimeWindow> dataWS = keyedStream.timeWindow(Time.seconds(5));
        WindowedStream<UserBehavior, Long, TimeWindow> dataWS = keyedStream.timeWindow(Time.seconds(50),Time.seconds(5));
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

        env.execute("Start_Time_Window");
    }
}
