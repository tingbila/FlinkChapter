package chapter08;



import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


//State Time-To-Live (TTL) 方法测试
public class StateTtlConfigCase {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8083);   //为了方便查询本地JobId信息
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("192.168.40.101", 9999, "\n");

        // parse the data, group it, window it, and aggregate the counts
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // normalize and split the line
                String[] tokens = value.toLowerCase().split("\\W+");

                // emit the pairs
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }).keyBy(value -> value.f0);


        SingleOutputStreamOperator<Tuple2<String, Integer>> processDataStream = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private ValueState<Tuple2<String, Integer>> wordCnt;
            private StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(10))      //第一个参数是强制性的，它是time-to-live值,即状态的生命周期
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)   //如何更新键控状态的最后访问时间:默认是每次写操作均更新State的最后访问时间
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  //状态可见性:设置是否返回过期但尚未清理的键控状态值,默认数值是永不返回过期状态
                    .build();

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<>("wordCnt", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}),Tuple2.of("", 0));
                descriptor.enableTimeToLive(ttlConfig);   //注意:这行代码一定要放在wordCnt = getRuntimeContext().getState(descriptor);的前面
                wordCnt = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> stringIntegerTuple2, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Tuple2<String, Integer> currentSum = wordCnt.value();
                if (currentSum  == null){
                    currentSum = Tuple2.of(context.getCurrentKey(),1);
                }else{
                    currentSum.f0 = context.getCurrentKey();
                    currentSum.f1 += 1;
                }
                wordCnt.update(currentSum);

                collector.collect(new Tuple2<>(currentSum.f0, currentSum.f1));
            }
        });

        processDataStream.print();

        env.execute("QueryableStateByStateDescriptor");
    }
}