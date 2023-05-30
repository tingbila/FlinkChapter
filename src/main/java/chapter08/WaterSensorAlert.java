package chapter08;


import bean.WaterSensor;
import lombok.val;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

// 以下代码展示了如何在一个 KeyedStream 上面使用 KeyedProcessFunction。 该函数对传感器温度进行监测，
// 如果某个传感器的温度在10秒的处理时间内持续上升则发出警告
public class WaterSensorAlert {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("heartbeat.timeout", "18000000");

        // 获取执行环境并设置配置
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

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
        KeyedStream<WaterSensor, String> keyedStream = mapDataStream.keyBy(value -> value.getId());  //以传感器id为键值进行分区
        DataStream<String> processDataStream = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            // 定义一个变量，保存上一次传感器对应的水位值(正常传感器的温度值应该都是大于0的).
            private ValueState<Integer> lastTemp;
            // 定义一个变量，用来记录是否已经注册过定时器,如果已经注册了定时器，此时变量的数值等于定时器设置的时间戳
            private ValueState<Long> isRegister;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastTemp", Integer.class, 0));
                isRegister = getRuntimeContext().getState(new ValueStateDescriptor<Long>("currentTimer", Long.class, 0L));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 获取传感器的上一次温度数值
                Integer prevTemp = lastTemp.value();
                // 传感器是否已经注册过定时器
                Long curTimerTimestamp = isRegister.value();

                // 如果当前传感器的温度值等于上一次温度,则直接跳过整次逻辑 eg:1 5 6 7 2 6
                if (prevTemp == 0){
                    System.out.println(ctx.getCurrentKey() + "传感器首次播报数据,直接略过!");
                }
                else if (value.getVc() < prevTemp) {
                    // 温度下降 删除当前计时器
                    ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp);
                    isRegister.clear();
                    System.out.println(ctx.getCurrentKey() + " 温度下降 删除当前计时器");
                } else if (value.getVc() > prevTemp && curTimerTimestamp == 0) {
                    //温度升高并且还未设置计时器
                    //以当前时间+5秒设置处理时间计时器
                    long timerTs = ctx.timerService().currentProcessingTime() + 10000;
                    ctx.timerService().registerProcessingTimeTimer(timerTs);
                    //记录当前的计时器
                    isRegister.update(timerTs);
                    System.out.println(ctx.getCurrentKey() + " 温度升高并且还未设置计时器");
                }

                // 更新传感器的最近一次的温度
                lastTemp.update(value.getVc());
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                //定时器触发,这里面也可以用Collector返回结果
                System.out.println("Temperature of sensor " + ctx.getCurrentKey() + " monotonically increased for 10 second.");
                //定时器触发完之后恢复初始化状态
                isRegister.clear();
            }
        });

        env.execute("WaterSensorAlert");
    }
}
