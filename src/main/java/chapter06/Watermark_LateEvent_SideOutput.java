package chapter06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

//在flink当中，对于晚于watermark的延迟数据，flink有3种处理方式：方式3：使用侧输出流，将延迟的数据统一收集和存储。
public class Watermark_LateEvent_SideOutput {
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

        //TODO 窗口延迟时间策略
        // 1.当watermark >= 窗口结束时间的时候，会正常触发计算，但是不会关闭窗口
        // 2.当watermark >= 窗口结束时间 + 窗口等待时间，窗口才会真正关闭
        // 2.当窗口结束时间 <= watermark < 窗口结束时间 + 窗口等待时间,每来一条迟到数据，窗口就会重新计算一次
        OutputTag<WaterSensor> lateDataOutputTag = new OutputTag<WaterSensor>("late data") {
        };
        SingleOutputStreamOperator<String> processData = mapDataStream
                .keyBy(value -> value.getId())
                .timeWindow(Time.seconds(5))               //[0,5)本应该在event_time >= 5秒时触发计算
                .allowedLateness(Time.seconds(2))          //设置窗口延迟司时间参数
                .sideOutputLateData(lateDataOutputTag)     //将延迟的数据写入到侧输出流
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                    @Override/**/
                    public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String windowStart = sdf.format(context.window().getStart());
                        String windowEnd = sdf.format(context.window().getEnd());
                        out.collect("Key:" + key + " 窗口范围[" + windowStart + "," + windowEnd + "] 单词统计次数: " + String.valueOf(elements.spliterator().estimateSize()));
                    }
                });

        processData.print();
        //注意:调用getSideOutput方法的不能是DataStream方法
        processData.getSideOutput(lateDataOutputTag).print("late data in side-output");

        env.execute("Watermark_LateEvent_SideOutput");
    }
}
