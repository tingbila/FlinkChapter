package util;

import bean.MarketingUserBehavior;
import bean.UserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {
    // 定义一个标志位，控制数据的产生
    private boolean flag = true;
    private List<String> behaviorList = Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
    private List<String> channelList = Arrays.asList("XIAOMI", "HUAWEI", "OPPO", "VIVO");

    @Override
    public void run(SourceFunction.SourceContext<MarketingUserBehavior> ctx) throws Exception {
        while (flag) {
            Random random = new Random();
            ctx.collect(
                    new MarketingUserBehavior(
                            //int number=r.nextInt(10) 获取数的范围：[0,10)
                            Long.valueOf(random.nextInt(10)),
                            behaviorList.get(random.nextInt(behaviorList.size())),
                            channelList.get(random.nextInt(channelList.size())),
                            System.currentTimeMillis()
                    )
            );
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
