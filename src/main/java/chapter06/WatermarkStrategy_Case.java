package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//The Deprecated AssignerWithPeriodicWatermarks and AssignerWithPunctuatedWatermarks
//flink1.12提供的WatermarkStrategy最新的API
public class WatermarkStrategy_Case {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // get input data by connecting to the socket
        DataStream<String> dataStreamSource = env.socketTextStream("192.168.40.101", 9999, "\n");
        DataStream<WaterSensor> mapDataStream = dataStreamSource.map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String line) throws Exception {
                String[] spited = line.split(",");
                return new WaterSensor(spited[0], Long.valueOf(spited[1]), Integer.valueOf(spited[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))  //数据的乱序时间
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000; //指定事件时间
                    }
                }));

//        .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
//            @Override
//            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
//                return element.getTs() * 1000; //指定事件时间
//            }
//        }));


        mapDataStream.print();
        DataStream<Long> processData = mapDataStream.keyBy(value -> value.getId()).timeWindow(Time.seconds(5))  //[0,5)本应该在event_time >= 5秒时触发计算
                .process(new ProcessWindowFunction<WaterSensor, Long, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<Long> out) throws Exception {
                        out.collect(elements.spliterator().estimateSize());
                    }
                });

        processData.print();

        env.execute("WatermarkStrategy_Case");
    }
}
