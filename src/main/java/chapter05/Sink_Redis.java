package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import util.MyUserBehaviorSource;


//场景:统计每个用户的浏览次数,然后将统计数据实时写入到Redis
public class Sink_Redis {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStreamSource<UserBehavior> dataStreamSource = env.addSource(new MyUserBehaviorSource());
        // 只保留pv行为
        DataStream<UserBehavior> filterDataStream = dataStreamSource.filter(value -> value.getBehavior().equals("pv"));
        // pv计数
        DataStream<Tuple2<String, Long>> mapData = filterDataStream.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of(String.valueOf(value.getUserId()), 1L);
            }
        });
        // keyBy然后sum求和
        KeyedStream<Tuple2<String, Long>, String> keyedStream = mapData.keyBy(value -> String.valueOf(value.f0));
        DataStream<Tuple2<String, Long>> sumData = keyedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });

        sumData.print();

        //将统计数据写入到redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("192.168.40.101").setPort(6379).build();
        sumData.addSink(new RedisSink<Tuple2<String, Long>>(conf, new RedisExampleMapper()));

        env.execute("Sink_Redis");
    }

    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, Long>> {
        //指定Redis的类型
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);  // 部分情况还需要第二个参数,这里面略。additionalKey additional key for Hash and Sorted set data type
        }

        @Override
        public String getKeyFromData(Tuple2<String, Long> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Long> data) {
            return String.valueOf(data.f1);
        }
    }
}
