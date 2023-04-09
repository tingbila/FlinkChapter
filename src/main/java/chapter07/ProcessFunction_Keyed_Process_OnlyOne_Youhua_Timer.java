package chapter07;

//EventTime时间-Timer定时器-测试案例
//我们应该在程序里面通过标志位来控制一个key最多只对应一个定时器！！
//优化点:解决不同key对应的状态state隔离的问题.

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

import java.sql.Timestamp;

public class ProcessFunction_Keyed_Process_OnlyOne_Youhua_Timer {
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
            private ValueState<Long> triggerTs = null; //每个key拥有隔离的valueState

            @Override
            public void open(Configuration parameters) throws Exception {
                triggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("valueState", Long.class, 0L));
            }

            //来一条数据，处理一条数据，类比MR当中Mapper当中的map方法.
            //只有当算子的watermark的时间大于定时器的时间，才会触发定时器。
            //为了避免重复注册定时器，重复创建对象，注册定时器的时候，判断一下是否已经注册过了定时器。
            //triggerTs.value()和 triggerTs.update 底层调用类似HashMap的代码,和key进行绑定
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //为了避免重复注册定时器，重复创建对象，注册定时器的时候，判断一下是否已经注册过了定时器。
                if (triggerTs.value() == 0) {   //value = triggerTs.stateTable.get(this.keyContext.getCurrentKey(), this.keyContext.getCurrentKeyGroupIndex(), namespace)
                    Long currentEventTime = value.getTs() * 1000;
                    System.out.println("当前event时间是: " + new Timestamp(currentEventTime));
                    ctx.timerService().registerEventTimeTimer(currentEventTime + 5000);
                    triggerTs.update(currentEventTime);  //triggerTs.stateTable.put(this.keyContext.getCurrentKey(), this.keyContext.getCurrentKeyGroupIndex(), namespace, value)
                }
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
                System.out.println("主人,主人,主人...快起床了,当前时间是:" + new Timestamp(timestamp) + "触发的watermark时间是: " + ctx.timerService().currentWatermark());
                //定时器触发完之后恢复初始化状态
                triggerTs.update(0L);
            }
        });

        env.execute("ProcessFunction_Keyed_Process_OnlyOne_Youhua_Timer");
    }
}
