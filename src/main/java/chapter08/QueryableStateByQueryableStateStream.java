package chapter08;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class QueryableStateByQueryableStateStream {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8083);
        //启用Queryable State服务,底层对应的就是:queryable-state.enable
        config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("192.168.40.101", 9999, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<Tuple2<String, Integer>> sumData = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
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
                }).keyBy(value -> value.f0)
                .sum(1); //滚动聚合算子

        // 使得结果的状态可查
        // asQueryableState 返回 QueryableStateStream
        // QueryableStateStream类似于一个接收器，无法进行进一步转换
        // QueryableStateStream接收传入的数据并更新状态
        QueryableStateStream<String, Tuple2<String, Integer>> wordCnt = sumData.keyBy(value -> value.f0).asQueryableState("query-name");

        sumData.print();

        env.execute("QueryableStateByQueryableStateStream");
    }
}