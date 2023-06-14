package chapter08;


import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;


public class QueryableStateStreamByQueryClient {
    public static void main(String[] args) throws Exception {
        //1. 提供任意一个TaskManager的主机名以及可查询式状态的代理监听端口,默认监听端口是9069
        QueryableStateClient client = new QueryableStateClient("localhost", 9069);

        String queryKey = "sensor_1";

        //2. 通过client.getKvState方法来获取具体key对应的value状态信息
        while (true) {
            try {
                CompletableFuture<ValueState<Integer>> resultFuture1 = client.getKvState(
                        JobID.fromHexString("1a620164f6b6cc0ecba02494821a92d5"),
                        "query-name",  // queryable state name
                        queryKey,
                        BasicTypeInfo.STRING_TYPE_INFO,   // key的类型
                        new ValueStateDescriptor<>("lastTemp", Integer.class));   //状态的名称和类型

                System.out.println(resultFuture1.get().value());
            } catch (Exception e) {
                System.out.println("获取状态失败: " + e.getMessage());  //无状态的时候会调用这个方法
            }

//            try {
//                CompletableFuture<ValueState<Long>> resultFuture2 = client.getKvState(
//                        JobID.fromHexString("ea772d3675227e646c42c33a2660a62a"),
//                        "query-name-2",  // queryable state name
//                        queryKey,
//                        BasicTypeInfo.STRING_TYPE_INFO,   // key的类型
//                        new ValueStateDescriptor<>("lastTimerState", Long.class));   //状态的名称和类型
//
//                System.out.println(resultFuture2.get().value());
//            } catch (Exception e) {
//                System.out.println("获取状态失败: " + e.getMessage());  //无状态的时候会调用这个方法
//            }

            Thread.sleep(1000);
        }
    }
}


