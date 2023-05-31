package chapter08;

import bean.WaterSensor;
import javafx.collections.ListChangeListener;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


//利用ListCheckpointed接口实现一个函数，其作用是在将元素达到指定阈值时在发出,实现缓冲元素的作用。
//参考官网的例子,在官网当中缓存元素的类型是:Tuple2<String, Integer>
public class OperatorStateCase {
    public static void main(String[] args) throws Exception {
        // get the execution environment
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8083);   //为了方便查询本地JobId信息
        config.setString("heartbeat.timeout", "18000000");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // get input data by connecting to the socket
        DataStream<String> dataStreamSource = env.socketTextStream("192.168.40.101", 9999, "\n");
        DataStream<WaterSensor> mapDataStream = dataStreamSource.map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String line) throws Exception {
                String[] spited = line.split(",");
                return new WaterSensor(spited[0], Long.valueOf(spited[1]), Integer.valueOf(spited[2]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(2)) {  //数据最大乱序时间
            @Override
            public long extractTimestamp(WaterSensor element) {   //watermark = max(event_time) - maxOutOfOrderness 看源代码
                return element.getTs() * 1000; //指定事件时间
            }
        });

        mapDataStream.print();

        SingleOutputStreamOperator<WaterSensor> flattedMapStream = mapDataStream.flatMap(new BufferingElement(3));
        flattedMapStream.print();

        env.execute("OperatorStateCase");
    }


    //这个类包含了2部分代码:正常的执行逻辑 + operator状态恢复
    public static class BufferingElement extends RichFlatMapFunction<WaterSensor, WaterSensor> implements CheckpointedFunction {
        private final int threshold;    //配置缓冲元素的阈值
        private List<WaterSensor> bufferedElements;   //存储缓冲的元素
        private transient ListState<WaterSensor> checkpointedState;   //用于实现Operator State

        public BufferingElement(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void flatMap(WaterSensor value, Collector<WaterSensor> out) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (WaterSensor element : bufferedElements) {
                    // send it to the sink
                    System.out.println("---------------start--------------->");
                    System.out.println(getRuntimeContext().getIndexOfThisSubtask());
                    out.collect(element);
                    System.out.println("<---------------end---------------");
                }
                bufferedElements.clear();
            }
        }

        //snapshotState方法会在执行checkpoint的时候被回调.
        //Checkpoint时会调用这个方法，我们要实现具体的snapshot逻辑，比如将哪些本地状态数据持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            for (WaterSensor element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        //initializeState方法会在初始化函数状态时调用，该过程可能发生在作业启动（无论是否从保存点启动）或故障恢复的情况下被回调，进而恢复算子状态。
        //初始化时会调用这个方法，向本地状态中填充数据（任务重启的时候也可能会调用这个方法）
        //we use the isRestored() method of the context to check if we are recovering after a failure. If this is true,
        //i.e. we are recovering, the restore logic is applied.
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<WaterSensor> descriptor = new ListStateDescriptor<>("buffered-elements", TypeInformation.of(WaterSensor.class));
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                for (WaterSensor element : checkpointedState.get()) {
                    bufferedElements.add(element);  //恢复算子的状态,向本地状态中填充数据
                }
            }
        }
    }
}
