package chapter08;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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



public class QueryableStateByStateDescriptor {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8083);
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

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

        keyedStream.print();

        SingleOutputStreamOperator<Tuple2<String, Integer>> processDataStream = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private ValueState<Tuple2<String, Integer>> wordCnt;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<>("wordCnt", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}),Tuple2.of("", 0));
                // 通过descriptor.setQueryable 开放此状态,使此状态可查询
                descriptor.setQueryable("query-name");
                wordCnt = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> stringIntegerTuple2, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Tuple2<String, Integer> currentSum = wordCnt.value();
                currentSum.f0 = context.getCurrentKey();
                currentSum.f1 += 1;
                wordCnt.update(currentSum);

                collector.collect(new Tuple2<>(currentSum.f0, currentSum.f1));
            }
        });

        processDataStream.print();

        env.execute("QueryableStateByStateDescriptor");
    }
}