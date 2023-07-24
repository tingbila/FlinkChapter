package chapterother;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;


//Flink流处理案例开发:Histogram直方图数据分布
public class SocketWordCountHistogram1 {
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
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(2)) {  //数据最大乱序时间
            @Override
            public long extractTimestamp(WaterSensor element) {   //watermark = max(event_time) - maxOutOfOrderness 看源代码
                return element.getTs() * 1000; //指定事件时间
            }
        });

        DataStream<WaterSensor> streamOperator = mapDataStream.map(new RichMapFunction<WaterSensor, WaterSensor>() {
            private transient Histogram histogram;
            @Override
            public void open(Configuration parameters) throws Exception {
                MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
                MetricGroup myGroup = metricGroup.addGroup("MyGroup");  //counter组
                histogram = myGroup.histogram("myHistogram", new DescriptiveStatisticsHistogram(1000)); //Flink中代码提供了内置实现描述性统计直方图，需要一个窗口大小的参数。
            }
            @Override
            public WaterSensor map(WaterSensor value) throws Exception {
                histogram.update(Long.parseLong(value.getVc().toString()));
                return value;
            }
        });

        streamOperator.print();

        env.execute("SocketWordCountHistogram1");
    }
}
