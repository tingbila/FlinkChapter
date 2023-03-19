package chapter05;

import bean.UserBehavior;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.MyUserBehaviorSource;

//基于滚动聚合算子进行计算
public class Transform_RollingAgg {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 1.Source：从自定义数据源读取
        DataStreamSource<UserBehavior> dataStreamSource = env.addSource(new MyUserBehaviorSource());

        KeyedStream<UserBehavior, Long> keyedStream = dataStreamSource.keyBy(value -> value.getUserId());
        // min和minBy的区别是：min返回指定字段的最小值，并将该值赋值给第一条数据并返回第一条数据,而minBy返回最小值所在的那条原生记录，其余同理。
        SingleOutputStreamOperator<UserBehavior> streamOperatorDs = keyedStream.minBy("timestamp");
        streamOperatorDs.print();

        env.execute("Transform_RollingAgg");
    }
}
