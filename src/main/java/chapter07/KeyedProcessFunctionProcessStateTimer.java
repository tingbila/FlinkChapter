package chapter07;

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

public class KeyedProcessFunctionProcessStateTimer {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // get input data by connecting to the socket
        DataStream<String> dataStreamSource = env.socketTextStream("localhost", 9999, "\n");
        DataStream<WaterSensor> mapDataStream = dataStreamSource.map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String line) throws Exception {
                String[] spited = line.split(",");
                return new WaterSensor(spited[0], Long.valueOf(spited[1]), Integer.valueOf(spited[2]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(WaterSensor element) {
                return element.getTs() * 1000;
            }
        });

        mapDataStream.print();
        KeyedStream<WaterSensor, String> keyedStream = mapDataStream.keyBy(value -> value.getId());
        DataStream<String> processDataStream = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            private ValueState<Long> timerState;

            @Override
            public void open(Configuration parameters) throws Exception {
                timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                Long triggerTs = timerState.value();
                if (triggerTs == null) {
                    ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000);
                    timerState.update(currentProcessingTime);
                } else {
                    System.out.println("already has a timer registered");
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("timer fired at " + new Timestamp(timestamp));
                timerState.clear();
            }
        });

        env.execute("KeyedProcessFunctionOnlyOneValueStateTimer");
    }
}
