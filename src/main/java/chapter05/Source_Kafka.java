package chapter05;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

//官网地址:https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/connectors/kafka.html

public class Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.Source：从 kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.40.101:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("topic1", new SimpleStringSchema(), properties));

        stream.print();

        env.execute("Source_Kafka");
    }
}
