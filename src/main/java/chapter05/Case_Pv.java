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

//需求:基于用户浏览信息统计总浏览pv
public class Case_Pv {
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
        // pv计数
        DataStream<Tuple2<String, Long>> mapData = filterDataStream.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", 1L);  //因为计算是总浏览pv,所以key设置成一样，保证下游被同一个算子进行计算处理。
            }
        });
        KeyedStream<Tuple2<String, Long>, String> keyedStream = mapData.keyBy(value -> value.f0);

        //方法1:DataStream<Tuple2<String, Long>> sumPvData = keyedStream.sum(1);   //sum点击源码查看,是哪个类的

        //方法2:
        SingleOutputStreamOperator<String> sumPvData = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
            // 定义一个变量，来统计条数
            private Long pvCount = 0L;

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                pvCount += 1;
                out.collect("pv是" + String.valueOf(pvCount));
            }
        });

        sumPvData.print();

        env.execute("Case_Pv");
    }
}
