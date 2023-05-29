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


// 对每个传感器进行状态计算,如果某个传感器上报的次数达到2次,将对这2次传感器水位线求平均值，然后状态清零。
// Once the count reaches 2 it will emit the waterSensor average and clear the state
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

        SingleOutputStreamOperator<Tuple2<Integer, Double>> alertStream = keyedStream.flatMap(new CountWindowAverage(2));
        alertStream.print();

        env.execute("CountWindowAverageState");
    }


    //使用键值分区状态的函数必须作用在KeyedStream上，即在应用函数之前需要在输入流上调用keyBy()方法来指定键值。对于一个作用在键值分区输入上
    //的函数而言，Flink运行时在调用它的处理方法时，会自动将函数中的键值分区状态对象放入到当前处理记录的键值上下文中。这样，函数每次只能访问
    //属于当前处理记录的键值的状态,注意:下面必须用RichFlatMapFunction,不能用FlatMapFunction
    public static class CountWindowAverage extends RichFlatMapFunction<WaterSensor, Tuple2<Integer, Double>> {
        private Integer threshold;  //配置上报次数

        public CountWindowAverage(Integer threshold) {
            this.threshold = threshold;
        }

        //状态引用对象
        //The ValueState handle. The first field is the count, the second field a running sum.
        private transient ValueState<Tuple2<Integer, Double>> sum;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 创建状态描述符:每个状态原语都有自己特定的StateDescriptor，它里面包含了状态名称和类型。
            // 状态处理的数据类型可以通过class或TypeInformation对象指定
            // default value of the state, if nothing was set
            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor = new ValueStateDescriptor<>("average", TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {}), Tuple2.of(0, 0));
            // 通过descriptor.setQueryable 开放此状态,使此状态可查询
            descriptor.setQueryable("query-name");
            // 获得状态引用
            sum = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(WaterSensor value, Collector<Tuple2<Integer, Double>> out) throws Exception {
            //从状态中获取上一次的温度
            Tuple2<Integer, Double> currentSum = sum.value();
            // update the count
            currentSum.f0 += 1;
            // add the second field of the input value
            currentSum.f1 += value.getVc();

            // update the state
            sum.update(currentSum);

            // if the count reaches 2, emit the average and clear the state
            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(currentSum.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }
    }
}
