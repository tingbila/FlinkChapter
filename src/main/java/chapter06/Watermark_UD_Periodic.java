package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class Watermark_UD_Periodic {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //每5秒生成一次水位线
        env.getConfig().setAutoWatermarkInterval(10000);

        // get input data by connecting to the socket
        DataStream<String> dataStreamSource = env.socketTextStream("192.168.40.101", 9999, "\n");
        DataStream<WaterSensor> mapDataStream = dataStreamSource.map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String line) throws Exception {
                String[] spited = line.split(",");
                return new WaterSensor(spited[0], Long.valueOf(spited[1]), Integer.valueOf(spited[2]));
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<WaterSensor>() {
            //自己实现的话需要保证watermark不要回退
            //代码仿照BoundedOutOfOrdernessTimestampExtractor的源码编写即可
            private long maxOutOfOrderness = 2 * 1000L;   //自定义乱序时间
            private long currentMaxTimestamp = 0;
            private long lastEmittedWatermark = 0;

            //这个方法的调用时间取决于env.getConfig().setAutoWatermarkInterval(x)方法,默认是2秒。
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                System.out.println("Periodic...Generate...Watermark");
                // this guarantees that the watermark never goes backwards.
                long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
                if (potentialWM >= lastEmittedWatermark) {
                    lastEmittedWatermark = potentialWM;
                }
                return new Watermark(lastEmittedWatermark);
            }

            //这个方法每来一条event事件就会调用一次
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                long timestamp = element.getTs() * 1000L;
                if (timestamp > currentMaxTimestamp) {
                    currentMaxTimestamp = timestamp;
                }
                return timestamp;
            }
        });

        mapDataStream.print();
        DataStream<Long> processData = mapDataStream.keyBy(value -> value.getId()).timeWindow(Time.seconds(5))  //[0,5)本应该在event_time >= 5秒时触发计算
                .process(new ProcessWindowFunction<WaterSensor, Long, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<Long> out) throws Exception {
                        out.collect(elements.spliterator().estimateSize());
                    }
                });

        processData.print();

        env.execute("Watermark_UD_Periodic");
    }
}
