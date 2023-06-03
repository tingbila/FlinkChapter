package chapter08;


import bean.UserBehavior;
import bean.WaterSensor;
import bean.WaterThreshold;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
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
        // get the execution environment
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8083);   //为了方便查询本地JobId信息
        config.setString("heartbeat.timeout", "18000000");
        //启用Queryable State服务,底层对应的就是:queryable-state.enable
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
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
        MapStateDescriptor<String, Integer> thresholdsDescriptor = new MapStateDescriptor<>("thresholds", String.class, Integer.class);
        //2. 将其中的阈值流注册成广播流
        BroadcastStream<WaterThreshold> broadcastThresholds = thresholdsStream.broadcast(thresholdsDescriptor);
        //3. 通过connect连接主流和广播流(连接键值分区传感器水位流和广播的规则流)
        BroadcastConnectedStream<WaterSensor, WaterThreshold> connectDataStream = keyedStream.connect(broadcastThresholds);

        DataStream<Tuple3<String, Integer, Integer>> processDataStream = connectDataStream.process(new KeyedBroadcastProcessFunction<String, WaterSensor, WaterThreshold, Tuple3<String, Integer, Integer>>() {
            // 定义一个变量，保存上一次传感器对应的水位值(键值分区状态引用对象).
            private ValueState<Integer> lastTemp;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("lastTemp", Integer.class, 0); //状态描述符
                // 通过descriptor.setQueryable 开放此状态,使此状态可查询
                descriptor.setQueryable("query-name-1");
                lastTemp = getRuntimeContext().getState(descriptor); //获取状态引用对象
            }

            @Override
            public void processElement(WaterSensor value, ReadOnlyContext ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                //获取只读的广播状态
                ReadOnlyBroadcastState<String, Integer> thresholds = ctx.getBroadcastState(thresholdsDescriptor);
                //检查阈值是否已经存在
                if (thresholds.contains(value.getId())) {
                    //获取指定传感器的阈值
                    Integer sensorThreshold = thresholds.get(value.getId());
                    //从状态中获取该传感器上一次的温度
                    Integer lastVc = lastTemp.value();
                    //检查是否需要发出警报
                    int tempDiff = Math.abs(value.getVc() - lastVc);
                    if (tempDiff > sensorThreshold) {
                        //传感器增加超过阈值
                        out.collect(Tuple3.of(value.getId(), value.getVc(), tempDiff));
                    }
                }
                //更新lastWaterSensorVc的状态
                lastTemp.update(Integer.valueOf(String.valueOf(value.getVc())));
            }

            @Override
            public void processBroadcastElement(WaterThreshold value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                //获取广播状态引用对象
                BroadcastState<String, Integer> thresholds = ctx.getBroadcastState(thresholdsDescriptor);
                if (value.getThresholdVc() != 0) {
                    //为指定传感器配置新的阈值
                    thresholds.put(value.getId(), value.getThresholdVc());
                } else {
                    thresholds.remove(value.getId());
                }
            }
        });

        processDataStream.print();

        env.execute("KeyedBroadcastProcessFunctionCase");
    }
}


