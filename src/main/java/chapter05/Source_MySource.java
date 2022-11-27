package chapter05;

import bean.UserBehavior;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import util.MySourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;


public class Source_MySource {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 1.Source：从自定义数据源读取
        DataStreamSource<UserBehavior> dataStreamSource = env.addSource(new MySourceFunction());

        dataStreamSource.print();

        env.execute("Source_MySource");
    }
}
