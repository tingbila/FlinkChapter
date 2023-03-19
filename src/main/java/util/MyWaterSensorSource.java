package util;


//有两种方式可以实现自定义数据源:
/*
通过实现SourceFunction接口来自定义无并行度（也就是并行度只能为1）的数据源；
通过实现ParallelSourceFunction接口或者继承RichParallelSourceFunction类来自定义有并行度的数据源。
*/

import bean.WaterSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MyWaterSensorSource implements SourceFunction<WaterSensor> {
    // 定义一个标志位，控制数据的产生
    private boolean flag = true;

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        //int number=r.nextInt(10) 获取数的范围：[0,10)
        Random random = new Random();

        while (flag) {
            ctx.collect(new WaterSensor(
                    "WaterSensor" + "_" + String.valueOf(random.nextInt(6)),  //[0,5]个传感器
                    System.currentTimeMillis(),   //时间戳
                    random.nextInt(100)    //水位线高度
            ));
            Thread.sleep(2000L);
        }

    }

    @Override
    public void cancel() {
        this.flag = false;
    }
}