package chapterother;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;


//对外提供当前水位线的WatermarkGauge指标的实现。
public class SocketWordCountGauge2 {
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

        mapDataStream.print();
        DataStream<String> processData = mapDataStream
                .keyBy(value -> value.getId())
                .timeWindow(Time.seconds(5))  //[0,5)本应该在event_time >= 5秒时触发计算
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    private long  currentWatermark = Long.MIN_VALUE;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MetricGroup metricGroup = getRuntimeContext().getMetricGroup();
                        MetricGroup myGroup = metricGroup.addGroup("MyGroup");  //指标组
                        myGroup.gauge("MyGaugeWatermark", new Gauge<Long>() {   //指标名称
                            @Override
                            public Long getValue() {
                                return currentWatermark;   //返回指标瞬时值
                            }
                        });
                    };

                    @Override
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        this.currentWatermark = context.currentWatermark();   //只有当窗口触发的时候,Gauge指标才会更新,这个注意一下!!
                        System.out.println(this.currentWatermark);
                        String windowStart = sdf.format(context.window().getStart());
                        String windowEnd = sdf.format(context.window().getEnd());
                        out.collect("Key:" + key + " 窗口范围[" + windowStart + "," + windowEnd + "] 单词统计次数: " + String.valueOf(elements.spliterator().estimateSize()));
                    }
                });

        processData.print();

        env.execute("SocketWordCountGauge2");
    }
}
