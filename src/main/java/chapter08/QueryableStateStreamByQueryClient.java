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
        //1. 提供任意一个TaskManager的主机名以及可查询式状态的代理的监听端口,客户端代理的默认监听端口是9067
        QueryableStateClient client = new QueryableStateClient("localhost", 9069);

        String queryKey = "hello";
        ValueStateDescriptor<Tuple2<String, Integer>> stateDescriptor =
                new ValueStateDescriptor<>(
                        "query-name",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        }));  //状态的类型

        //2. 通过client.getKvState方法来获取具体key对应的value状态信息
        while (true) {
            CompletableFuture<ValueState<Tuple2<String, Integer>>> resultFuture = client.getKvState(
                    JobID.fromHexString("707fbba47bdc81404523b70d14001831"),
                    "query-name",
                    queryKey,
                    BasicTypeInfo.STRING_TYPE_INFO,
                    stateDescriptor);

            System.out.println(resultFuture.get().value());

            Thread.sleep(1000);
        }
    }
}


