package chapter05;

import bean.UserBehavior;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.MyUserBehaviorSource;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Sink_Mysql {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStreamSource<UserBehavior> input = env.addSource(new MyUserBehaviorSource());
        input.print();

        input.addSink(JdbcSink.sink("insert into userstatics (user_id, item_id, behavior) values (?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.getUserId().intValue());
                    ps.setString(2, String.valueOf(t.getItemId()));
                    ps.setString(3, t.getBehavior());
                },
                //官网没有这个加这个,为了方便测试,我加的这个参数
                JdbcExecutionOptions.builder()
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.40.102:3306/userinfo?characterEncoding=utf8")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456").build()
        ));

        env.execute("Sink_Mysql");
    }
}


