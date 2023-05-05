package chapter07;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

//系统时间-Timer定时器-测试案例

public class KeyedProcessFunctionProcessTimer {
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
        KeyedStream<WaterSensor, String> keyedStream = mapDataStream.keyBy(value -> value.getId());
        DataStream<String> processDataStream = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //来一条数据，处理一条数据，类比MR当中Mapper当中的map方法.
            //在这里需要注意的是:在这里设置的定时器是系统时间的定时器，虽然时间语义是eventTime，但是只要系统时间到达了，就会触发，和eventTime没有关系。
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                System.out.println("当前系统时间是: " + new Timestamp(currentProcessingTime));
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000);
            }

            /**
             * 到了定时的时间要干什么事情
             * @param timestamp   定时器设置的时间,可以理解为闹钟响的时间.
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("主人,主人,主人...快起床了,当前时间是:" + new Timestamp(timestamp));
            }
        });

        env.execute("KeyedProcessFunctionProcessTimer");
    }
}























