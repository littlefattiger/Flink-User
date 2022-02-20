package com.atguigu.market_analysis;

import com.atguigu.market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AppMarketingByChannel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStream<MarketingUserBehavior> dataStream = env.addSource(new SimulatedMarketingBehaviorSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<MarketingUserBehavior>forMonotonousTimestamps().withTimestampAssigner(
                        (SerializableTimestampAssigner<MarketingUserBehavior>) (element, recordTimeStamp) -> element.getTimestamp() * 1000
                )
        );

        dataStream
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                        .keyBy(item->)
        env.execute();


    }

    public static class SimulatedMarketingBehaviorSource implements
            SourceFunction<MarketingUserBehavior> {
        Boolean running = true;

        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL",
                "UNINSTALL");
        List<String> channelList = Arrays.asList("app store", "weibo", "wechat",
                "tieba");
        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (running) {
                Long id = random.nextLong();
                String behavior =
                        behaviorList.get(random.nextInt(behaviorList.size()));
                String channel =
                        channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();
                ctx.collect(new MarketingUserBehavior(id, behavior, channel,
                        timestamp));
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
}
