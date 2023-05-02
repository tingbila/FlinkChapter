package chapter08;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.elasticsearch.common.recycler.Recycler;

import java.sql.Timestamp;

//需求：监控温度传感器的温度值，如果传感器温度值在5秒钟之内(event time)连续上升，则报警
//watermark和分组没有关系
//深究的话程序还有很多问题!!!!!
public class WaterSensor_Alert_Youhua {
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

        //mapDataStream.print();
        KeyedStream<WaterSensor, String> keyedStream = mapDataStream.keyBy(value -> value.getId());
        DataStream<String> processDataStream = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            //定义一个变量，保存上一次的水位值.
            private ValueState<Integer> lastWaterSensorValue;
            //定义一个变量，用来记录是否已经注册过定时器.
            private ValueState<Long> isRegister;  //这里用Integer会有问题。。。需要和注册时的类型保持一致..

            @Override
            public void open(Configuration parameters) throws Exception {
                lastWaterSensorValue = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastWaterSensorValue",Integer.class,Integer.MIN_VALUE));
                isRegister = getRuntimeContext().getState(new ValueStateDescriptor<Long>("isRegister",Long.class,0L));
            }

            //来一条数据，处理一条数据，类比MR当中Mapper当中的map方法.
            //只有当算子的watermark的时间大于定时器的时间，才会触发定时器。
            //为了避免重复注册定时器，重复创建对象，注册定时器的时候，判断一下是否已经注册过了定时器。
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                if (isRegister.value() == 0) { //如果当前key还没有注册定时器
                    ctx.timerService().registerEventTimeTimer(value.getTs() * 1000L + 5000L);
                    System.out.println("注册定时器时间是: " + new Timestamp(value.getTs() * 1000L));
                    isRegister.update(value.getTs() * 1000L + 5000L);
                } else {
                    if (value.getVc() <= lastWaterSensorValue.value()) {
                        //删除之前注册的定时器
                        ctx.timerService().deleteEventTimeTimer(isRegister.value());
                        //重新注册定时器
                        ctx.timerService().registerEventTimeTimer(value.getTs() * 1000L + 5000L);
                        System.out.println("重新注册定时器的时间是: " + new Timestamp(value.getTs() * 1000L));
                        isRegister.update(value.getTs() * 1000L + 5000L);
                    }
                }
                //不管上升还是下降，都要保存水位值，供下条数据使用，进行比较
                lastWaterSensorValue.update(value.getVc());
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
                //定时器触发，说明已经满足连续5秒水位上升
                System.out.println("主人,主人,主人...监测到水位连续5s上升,当前时间是:" + new Timestamp(timestamp) + "触发的watermark时间是: " + ctx.timerService().currentWatermark());
                //定时器触发完之后恢复初始化状态
                isRegister.update(0L);
            }
        });

        env.execute("WaterSensor_Alert");
    }
}
