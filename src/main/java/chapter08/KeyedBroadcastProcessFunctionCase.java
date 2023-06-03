package chapter08;


import bean.UserBehavior;
import bean.WaterSensor;
import bean.WaterThreshold;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import util.MyUserBehaviorSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


// 展示了一个KeyedBroadcastProcessFunction的实现:支持在运行时动态配置传感器阈值。
public class KeyedBroadcastProcessFunctionCase {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("heartbeat.timeout", "18000000");

        // 获取执行环境并设置配置
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 获取阈值流 {"sensor_1": 10, "sensor_1": 15}
        DataStream<String> dataStreamSource1 = env.socketTextStream("192.168.40.101", 9998, "\n");
        DataStream<WaterThreshold> thresholdsStream = dataStreamSource1.flatMap(new FlatMapFunction<String, WaterThreshold>() {
            @Override
            public void flatMap(String value, Collector<WaterThreshold> out) throws Exception {
                try {
                    WaterThreshold jsonValue = JSON.parseObject(value, WaterThreshold.class);
                    if (jsonValue != null) {
                        out.collect(jsonValue);
                    }
                } catch (Exception e) {
                    System.out.println("解析Json_WaterThreshold异常，异常信息是:" + e.getMessage());
                }
            }
        });


        // get input data by connecting to the socket
        DataStream<String> sensorData = env.socketTextStream("192.168.40.101", 9999, "\n");
        DataStream<WaterSensor> sensorDataStream = sensorData.map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String line) throws Exception {
                String[] spited = line.split(",");
                return new WaterSensor(spited[0], Long.valueOf(spited[1]), Integer.valueOf(spited[2]));
            }
        });
        KeyedStream<WaterSensor, String> keyedStream = sensorDataStream.keyBy(r -> r.getId());



        //1. 定义一个MapStateDescriptor来描述我们要广播的数据的格式(广播状态的描述符)
        MapStateDescriptor<String, WaterThreshold> descriptor = new MapStateDescriptor<>("thresholds", String.class, WaterThreshold.class);
        //2. 将其中的阈值流注册成广播流
        BroadcastStream<WaterThreshold> broadcastThresholds = thresholdsStream.broadcast(descriptor);
        //3. 通过connect连接主流和广播流(连接键值分区传感器水位流和广播的规则流)
        BroadcastConnectedStream<WaterSensor, WaterThreshold> connectDataStream = keyedStream.connect(broadcastThresholds);




        //2条数据流进行合并,并进行keyBy
        ConnectedStreams<WaterSensor, Tuple2<String, Long>> connectedStreams = sensorDataStream.connect(filterSwithes).keyBy(r1 -> r1.getId(), r2 -> r2.f0);
        DataStream<WaterSensor> processDataStream = connectedStreams.process(new CoProcessFunction<WaterSensor, Tuple2<String, Long>, WaterSensor>() {
            // 定义一个变量，作为转发开关
            private ValueState<Boolean> forwardingEnabled;
            // 定义一个变量，用于保存转发开关的停止计时器的时间
            private ValueState<Long> disableTimer;

            @Override
            public void open(Configuration parameters) throws Exception {
                forwardingEnabled = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("lastTemp", Boolean.class, false));
                disableTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class, 0L));
            }

            @Override
            public void processElement1(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //检查是否可以转发读数
                if (forwardingEnabled.value()) {
                    out.collect(value);
                }
            }

            @Override
            public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("Received filter switch: " + value);
                //开启读数转发
                forwardingEnabled.update(true);
                //设置停止计时器
                long timerTimestamp = ctx.timerService().currentProcessingTime() + value.f1;
                Long curTimerTimestamp = disableTimer.value();
                if (timerTimestamp > curTimerTimestamp) {
                    //移除当前计时器并注册一个新的定时器
                    //ctx.timerService().deleteEventTimeTimer(curTimerTimestamp);//感觉这里应该写错了
                    ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                    ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("Timer fired at timestamp: " + timestamp);
                //移除所有状态，默认情况下转发开关关闭
                forwardingEnabled.clear();
                disableTimer.clear();
            }
        });

        processDataStream.print();

        env.execute("KeyedBroadcastProcessFunctionCase");
    }
}


