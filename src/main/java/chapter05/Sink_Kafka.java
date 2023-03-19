package chapter05;

import bean.UserBehavior;
import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import util.MyUserBehaviorSource;
import util.MyWaterSensorSource;

import java.util.Properties;

public class Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从自定义数据源读取
        DataStreamSource<WaterSensor> dataStreamSource = env.addSource(new MyWaterSensorSource());
        DataStream<String> mapDataStream = dataStreamSource.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return value.toString();
            }
        });
        mapDataStream.print();

        // 将数据流写如到kafka，kafka0.11版本提供了Exactly-Once语义
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.40.101:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("topic1", new SimpleStringSchema(), properties);
        // DataStream调用 addSink => 注意，不是env来调用
        mapDataStream.addSink(kafkaProducer);

        env.execute("Sink_Kafka");
    }
}
