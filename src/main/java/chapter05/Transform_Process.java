package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.MyUserBehaviorSource;

public class Transform_Process {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStreamSource<UserBehavior> dataStreamSource = env.addSource(new MyUserBehaviorSource());
        // 只保留pv行为
        DataStream<UserBehavior> filterDataStream = dataStreamSource.filter(value -> value.getBehavior().equals("pv"));
        // pv计数
        DataStream<Tuple2<String, Long>> mapData = filterDataStream.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of(String.valueOf(value.getUserId()), 1L);
            }
        });

        // keyBy然后sum求和
        KeyedStream<Tuple2<String, Long>, String> keyedStream = mapData.keyBy(value -> String.valueOf(value.f0));
        keyedStream.print();

        /* processElement 来一条处理一条
         * @param <K> Type of the key.
         * @param <I> Type of the input elements.
         * @param <O> Type of the output elements.
         * Context通过源码查看上下文
         */
        DataStream<String> processData = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                out.collect("当前key=" + ctx.getCurrentKey() + "当前时间=" + ctx.timestamp() + ",数据=" + value);
            }
        });

        processData.print();

        env.execute("Transform_Process");
    }
}
