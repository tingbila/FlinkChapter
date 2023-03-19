package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.MyUserBehaviorSource;

public class Transform_Union {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从自定义数据源读取
        DataStreamSource<UserBehavior> dataStreamSource1 = env.addSource(new MyUserBehaviorSource());
        DataStreamSource<UserBehavior> dataStreamSource2 = env.addSource(new MyUserBehaviorSource());
        DataStreamSource<UserBehavior> dataStreamSource3 = env.addSource(new MyUserBehaviorSource());

        // 通过union算法来合并多个流
        DataStream<UserBehavior> unionDataStream = dataStreamSource1.union(dataStreamSource2, dataStreamSource3);
        unionDataStream.print();

        env.execute("Transform_Union");
    }
}
