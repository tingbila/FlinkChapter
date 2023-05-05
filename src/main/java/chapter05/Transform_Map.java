package chapter05;

import bean.UserBehavior;
import com.typesafe.sslconfig.ssl.FakeChainedKeyStore;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Transform_Map {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env.readTextFile("D:\\workspace\\idea_location\\FlinkChapter\\input\\UserBehavior.txt");

        // 2.Transform: Map转换成实体对象
        SingleOutputStreamOperator<UserBehavior> sensorDS = inputDS.map(new MyMapFunction());

        // 3.打印
        sensorDS.print();

        env.execute("Transform_Map");
    }

    public static class MyMapFunction implements MapFunction<String, UserBehavior> {
        @Override
        public UserBehavior map(String value) throws Exception {
            String[] datas = value.split(",");
            Long userId = Long.parseLong(datas[0]);
            Long itemId = Long.parseLong(datas[1]);
            Integer categoryId = Integer.parseInt(datas[2]);
            String behavior = datas[3];
            Long timestamp = Long.parseLong(datas[4]);
            return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
        }
    }
}
