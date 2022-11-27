package util;

import bean.UserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

//有两种方式可以实现自定义数据源:
/*
通过实现SourceFunction接口来自定义无并行度（也就是并行度只能为1）的数据源；
通过实现ParallelSourceFunction接口或者继承RichParallelSourceFunction类来自定义有并行度的数据源。
*/

public class MySourceFunction implements SourceFunction<UserBehavior> {
    // 定义一个标志位，控制数据的产生
    private boolean flag = true;
    private List<String> behaviorList = Arrays.asList("pv", "buy", "fav", "cart");

    @Override
    public void run(SourceContext<UserBehavior> ctx) throws Exception {
        Random random = new Random();

        while (flag) {
            ctx.collect(
                    new UserBehavior(
                            //int number=r.nextInt(10) 获取数的范围：[0,10)
                            Long.valueOf(random.nextInt(100000)),
                            Long.valueOf(random.nextInt(100)),
                            random.nextInt(50),
                            behaviorList.get(random.nextInt(behaviorList.size())),
                            System.currentTimeMillis()
                    ));
            Thread.sleep(2000L);
        }
    }

    @Override
    public void cancel() {
        this.flag = false;
    }
}
