package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.MySourceFunction;

public class Transform_RichMapFunction {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 从自定义数据源读取
        DataStreamSource<UserBehavior> dataStreamSource = env.addSource(new MySourceFunction());

        SingleOutputStreamOperator<String> DS = dataStreamSource.map(new MyMapFunction());

        DS.print();

        env.execute("Transform_RichMapFunction");
    }

    /**
     * 继承 RichMapFunction，指定输入的类型，返回的类型
     * 提供了 open()和 close() 生命周期管理方法
     * 能够获取 运行时上下文对象 =》 可以获取 状态、任务信息 等环境信息
     */
    public static class MyMapFunction extends RichMapFunction<UserBehavior, String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        @Override
        public String map(UserBehavior value) throws Exception {
//            String taskName = getRuntimeContext().getTaskName();
//            System.out.println(taskName);
            return value.toString();
        }

        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }
    }
}
