package chapter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import util.MyUserBehaviorSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sink_Es {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStreamSource<UserBehavior> input = env.addSource(new MyUserBehaviorSource());
        input.print();

        //ES连接信息
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.40.101", 9200, "http"));
        httpHosts.add(new HttpHost("192.168.40.101", 9200, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<UserBehavior> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<UserBehavior>() {
            @Override
            public void process(UserBehavior userBehavior, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                Map<String, String> json = new HashMap<>();
                //文档内容
                json.put("userID", String.valueOf(userBehavior.getUserId()));
                json.put("productID", String.valueOf(userBehavior.getItemId()));
                json.put("productPrice", userBehavior.getBehavior());

                IndexRequest source = Requests.indexRequest()
                        .index("viewlog")
                        .type("userbehavior")
                        .id(String.valueOf(userBehavior.getUserId()))   //指定主键
                        .source(json);

                requestIndexer.add(source);
            }
        });

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        input.addSink(esSinkBuilder.build());

        env.execute("Sink_Es");
    }
}
