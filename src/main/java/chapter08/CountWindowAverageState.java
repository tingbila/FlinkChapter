package chapter08;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


// 以下代码展示了传感器在检测到相邻温度值变化超过给定阈值时发出警报。
public class CountWindowAverageState {
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

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> alertStream = keyedStream.flatMap(new TemperatureAlertFunction(1.7));
        alertStream.print();

        env.execute("WaterSensoStateChangeAlert");
    }

    //在键值分区数据流上应用一个有状态的FlatMapFunction来比较读数并发出警报
    //使用键值分区状态的函数必须作用在KeyedStream上，即在应用函数之前需要在输入流上调用keyBy()方法来指定键值。对于一个作用在键值分区输入上
    //的函数而言，Flink运行时在调用它的处理方法时，会自动将函数中的键值分区状态对象放入到当前处理记录的键值上下文中。这样，函数每次只能访问
    //属于当前处理记录的键值的状态,注意:下面必须用RichFlatMapFunction,不能用FlatMapFunction
    public static class TemperatureAlertFunction extends RichFlatMapFunction<WaterSensor, Tuple3<String, Integer, Integer>> {
        private Double threshold;  //配置浮动阈值

        public TemperatureAlertFunction(Double threshold) {
            this.threshold = threshold;
        }

        //状态引用对象
        private ValueState<Integer> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建状态描述符:每个状态原语都有自己特定的StateDescriptor，它里面包含了状态名称和类型。
            // 状态处理的数据类型可以通过class或TypeInformation对象指定
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("lastTemp", Integer.class, 0);
            // 通过descriptor.setQueryable 开放此状态,使此状态可查询
            descriptor.setQueryable("query-name");
            // 获得状态引用
            lastTempState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(WaterSensor value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            //从状态中获取上一次的温度
            Integer lastTemp = lastTempState.value();
            //检查是否需要发出报警
            int tempDiff = Math.abs(value.getVc() - lastTemp);
            if (tempDiff > threshold) {
                //温度超过给定的阈值
                out.collect(Tuple3.of(value.getId(), value.getVc(), tempDiff));
            }
            //更新lastTemp状态
            lastTempState.update(value.getVc());
        }
    }
}















