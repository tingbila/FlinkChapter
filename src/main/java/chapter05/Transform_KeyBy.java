package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.MyUserBehaviorSource;

public class Transform_KeyBy {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 1.Source：从自定义数据源读取
        DataStreamSource<UserBehavior> dataStreamSource = env.addSource(new MyUserBehaviorSource());

        // 通过keyBy(new KeySelector {...})指定字段进行分区或者通过lambda表达式进行分区
        KeyedStream<UserBehavior, String> keyedStream = dataStreamSource.keyBy(value -> value.getBehavior());
//        KeyedStream<UserBehavior, String> keyedStream = dataStreamSource.keyBy(new KeySelector<UserBehavior, String>() {
//            @Override
//            public String getKey(UserBehavior value) throws Exception {
//                return value.getBehavior();
//            }
//        });
//        KeyedStream<UserBehavior, Tuple> keyedStream = dataStreamSource.keyBy("behavior");

        keyedStream.print();

        env.execute("Transform_KeyBy");
    }
}
