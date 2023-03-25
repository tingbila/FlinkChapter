package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import util.MyUserBehaviorSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


//自定义输出：addSink可以实现将数据输出到第三方的存储介质中，其中自定义Sink有2种实现方式：
//        a. 实现SinkFunction接口
//        b. 继承RichSinkFunction类


public class Sink_MySink {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStreamSource<UserBehavior> input = env.addSource(new MyUserBehaviorSource());
        input.print();

        input.addSink(new RichSinkFunction<UserBehavior>() {
            private Connection conn = null;
            private PreparedStatement ps = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                conn = DriverManager.getConnection("jdbc:mysql://192.168.40.102:3306/userinfo", "root", "123456");
                ps = conn.prepareStatement("insert into userstatics (user_id, item_id, behavior) values (?,?,?)");
            }

            @Override
            public void invoke(UserBehavior t, Context context) throws Exception {
                ps.setInt(1, t.getUserId().intValue());
                ps.setString(2, String.valueOf(t.getItemId()));
                ps.setString(3, t.getBehavior());
                ps.execute();
            }

            @Override
            public void close() throws Exception {
                ps.close();
                conn.close();
            }
        });

        env.execute("Sink_MySink");
    }
}













