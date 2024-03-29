package chapter06;

import bean.WaterSensor;
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

import java.text.SimpleDateFormat;


//TODO: 下游算子的watermark是取上游所有channel中watermark的最小值，只有最小watermark数值>=窗口的结束时间,才会触发窗口的执行。
public class Watermark_Parallalism {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);  //在这里设置多并行度
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

        mapDataStream.print();
        DataStream<String> processData = mapDataStream
                .keyBy(value -> value.getId())
                .timeWindow(Time.seconds(5))  //[0,5)本应该在event_time >= 5秒时触发计算
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String windowStart = sdf.format(context.window().getStart());
                        String windowEnd = sdf.format(context.window().getEnd());
                        out.collect("Key:" + key + " 窗口范围[" + windowStart + "," + windowEnd + "] 单词统计次数: " + String.valueOf(elements.spliterator().estimateSize()));
                    }
                }).setParallelism(5);

        processData.print();

        env.execute("Watermark_Parallalism");
    }
}
