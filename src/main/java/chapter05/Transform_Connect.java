package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import util.MyUserBehaviorSource;

import java.util.Properties;

public class Transform_Connect {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从自定义数据源读取
        DataStreamSource<UserBehavior> dataStreamSource1 = env.addSource(new MyUserBehaviorSource());


        // kafka数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.40.101:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> dataStreamSource2 = env.addSource(new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), properties));

        // 通过connect算子连接2个数据流
        ConnectedStreams<UserBehavior, String> connectedStreams = dataStreamSource1.connect(dataStreamSource2);

        // 不同的处理方法数据分开处理
        DataStream<String> mapDataStream = connectedStreams.map(new CoMapFunction<UserBehavior, String, String>() {
            @Override
            public String map1(UserBehavior value) throws Exception {
                return value.getBehavior();
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        mapDataStream.print();

        env.execute("Transform_Connect");
    }
}














