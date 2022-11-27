package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.MySourceFunction;

public class Transform_Filter {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 1.Source：从自定义数据源读取
        DataStreamSource<UserBehavior> dataStreamSource = env.addSource(new MySourceFunction());
        SingleOutputStreamOperator<UserBehavior> filterStream = dataStreamSource.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                // 只过滤pv行为的数据
                return "pv".equals(value.getBehavior());
            }
        });

        filterStream.print();

        env.execute("Transform_Filter");
    }
}
