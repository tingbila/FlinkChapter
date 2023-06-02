package chapter08;


import bean.WaterSensor;
import lombok.val;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

//通过KeyedProcessFunction对两个连续的传感器测量值进行比较，如果二者的差值大于一个特定的阈值就会发出警报，
//同时KeyedProcessFunction会在某一键值超过60秒（事件时间）都没有新到的传感器测量数据时将其对应的状态清除。
public class CleaningTemperatureAlert {
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

        DataStream<Tuple3<String, Integer, Integer>> processData = keyedStream.process(new SelfCleaningTemperatureAlert(10));
        processData.print();

        env.execute("CleaningTemperatureAlert");
    }

    public static class SelfCleaningTemperatureAlert extends KeyedProcessFunction<String, WaterSensor, Tuple3<String, Integer, Integer>> {
        private Integer threshold;  //配置告警阈值
        public SelfCleaningTemperatureAlert(Integer threshold) {
            this.threshold = threshold;
        }

        // 定义一个变量，保存上一次传感器对应的水位值(正常传感器的传感器值应该都是大于0的).
        private ValueState<Integer> lastWaterSensorVc;
        // 定义一个变量，用来记录是否已经注册过定时器,如果已经注册了定时器，此时变量的数值等于定时器设置的时间戳
        private ValueState<Long> lastTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> descriptor1 = new ValueStateDescriptor<>("lastWaterSensorVc", Integer.class, 0);
            descriptor1.setQueryable("query-name-1"); // 通过descriptor.setQueryable 开放此状态,使此状态可查询
            lastWaterSensorVc = getRuntimeContext().getState(descriptor1);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<>("lastTimerState", Long.class, 0L);
            descriptor2.setQueryable("query-name-2"); // 通过descriptor.setQueryable 开放此状态,使此状态可查询
            lastTimerState = getRuntimeContext().getState(descriptor2);
        }

        //该方法会通过删除已有计时器并注册新计时器的方法达到“延期清理”的目的。
        //清理时间被设置为比当前记录时间晚一分钟。
        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            // 获取当前定时器的时间戳,并进行删除
            Long curTimerTimestamp = lastTimerState.value();
            ctx.timerService().deleteEventTimeTimer(curTimerTimestamp);
            // 设置新的计时器（比当前记录时间戳晚一小时的时间）
            long newTimer = ctx.timestamp() + (60 * 1000);
            ctx.timerService().registerEventTimeTimer(newTimer);
            // 更新最新传感器定时器的时间戳
            lastTimerState.update(newTimer);


            // 获取传感器的上一次水位数值
            Integer prevTemp = lastWaterSensorVc.value();
            // 检查是否需要发出警报
            int tempDiff = Math.abs(value.getVc() - prevTemp);
            if (tempDiff > threshold) {
                //传感器增加超过阈值
                out.collect(Tuple3.of(value.getId(), value.getVc(), tempDiff));
            }

            //更新lastWaterSensorVc的状态
            lastWaterSensorVc.update(Integer.valueOf(String.valueOf(value.getVc())));
        }

        
        //该回调onTimer()方法会清除所有当前键值的状态，包括用于保存最近一次传感器以及前一个计时器时间的状态。
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            System.out.println("定时器响了,当前定时器时间是:" + timestamp + ":" + new Timestamp(timestamp));
            //清楚当前键值的所有状态
            lastWaterSensorVc.clear();
            lastTimerState.clear();
        }
    }
}
