package chapter05;

import bean.MarketingUserBehavior;
import bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import util.MarketingUserBehaviorSource;
import util.MyUserBehaviorSource;

/**
 * 对于电商企业来说，一般会通过各种不同的渠道对自己的APP进行市场推广，而这些渠道的统计数据（比如，不同网站上广告链接的点击量、
 * APP下载量）就成了市场营销的重要商业指标
 * 需求:统计不同渠道不同行为对应的推广数据
 */
public class Case_APPMarketingAnalysis {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 8083);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 读取数据
        DataStreamSource<MarketingUserBehavior> dataStreamSource = env.addSource(new MarketingUserBehaviorSource());

        DataStream<Tuple2<String, Long>> mapData = dataStreamSource.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                //在这里key进行了拼接。
                return Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1L);
            }
        });

        mapData.print();
        KeyedStream<Tuple2<String, Long>, String> keyedStream = mapData.keyBy(value -> value.f0);

        //方法1:DataStream<Tuple2<String, Long>> sumData = keyedStream.sum(1);   //sum点击源码查看,是哪个类的

        //方法2:
        DataStream<String> sumData = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
            // 定义一个变量，来统计条数
            private Long cnt = 0L;

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                cnt += 1;
                out.collect(value.f0 + "渠道投放数据:" + String.valueOf(cnt));
            }
        });

        sumData.print();

        env.execute("Case_APPMarketingAnalysis");
    }
}
