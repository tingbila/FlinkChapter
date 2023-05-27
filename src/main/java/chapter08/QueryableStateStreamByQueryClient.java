package chapter08;


import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

//方式1:通过Queryable State Stream使状态可查
public class QueryableStateStreamByQueryClient {
    public static void main(String[] args) throws Exception {
        //假设使用本地模式，TM和客户端运行在相同机器上
        String proxyHost = "localhost";
        int proxyPort = 9069;

        //运行QueryableStateJob的JobID,可以通过运行作业的日志或Web UI获取
        final JobID jobId = JobID.fromHexString("327be551c96af209083c65f41cdb5251");

        //利用可查询式状态代理的主机名和端口来配置客户端
        QueryableStateClient client = new QueryableStateClient(proxyHost, proxyPort);
        // the state descriptor of the state to be fetched.
        ValueStateDescriptor<Tuple2<String, Integer>> stateDescriptor =
                new ValueStateDescriptor<>(
                        "query-name",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        }));

        String queryKey = "hello";

        //轮询执行查询 queryableStateName，应和Server端的queryableStateName相同
        while (true) {
            CompletableFuture<ValueState<Tuple2<String, Integer>>> resultFuture = client.getKvState(
                    jobId,
                    "query-name",
                    queryKey,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    stateDescriptor);

            System.out.println(resultFuture.get().value());

            Thread.sleep(1000);
        }
    }
}


