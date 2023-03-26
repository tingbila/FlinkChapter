package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.MyUserBehaviorSource;

import java.util.HashSet;
import java.util.Set;

//需求:基于用户浏览信息统计总uv
//当前使用Set进行处理,后面我们使用布隆过滤器进行优化

public class Case_Uv {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 读取数据
        DataStreamSource<UserBehavior> dataStreamSource = env.addSource(new MyUserBehaviorSource());
        // 只保留pv行为
        DataStream<UserBehavior> filterDataStream = dataStreamSource.filter(value -> "pv".equals(value.getBehavior()));
        filterDataStream.print();
        // uv计数
        DataStream<Tuple2<String, Long>> mapData = filterDataStream.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("uv", value.getUserId());  //因为计算是总浏览pv,所以key设置成一样，保证下游被同一个算子进行计算处理。
            }
        });
        KeyedStream<Tuple2<String, Long>, String> keyedStream = mapData.keyBy(value -> value.f0);
        DataStream<String> UvData = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
            // 定义一个Set，用来去重并存放 userId,对用户id进行去重
            private final Set<Long> uvSet = new HashSet<>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                // 来一条数据，就把 userId存到 Set中
                uvSet.add(value.f1);
                out.collect("uv是" + String.valueOf(uvSet.size()));
            }
        });

        UvData.print();

        env.execute("Case_Uv");
    }
}
