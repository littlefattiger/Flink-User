package com.atguigu.networkflow_analysis;

import com.atguigu.networkflow_analysis.beans.PageViewCount;
import com.atguigu.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;

public class UvWithBloom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = HotPages.class.getResource("/apache.log");
        DataStream<String> inputStream = env.readTextFile("C:\\Users\\Junchi Lin\\IdeaProjects\\UserBehaviorAnalysis\\Hotpages\\src\\main\\resources\\UserBehavior.csv");
        DataStream<UserBehavior> dataStream = inputStream
                .map(
                        line -> {
                            String[] fields = line.split(",");
                            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                        }
                ).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>) (element, recordTimeStamp) -> element.getTimestamp() * 1000));

        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .trigger(new MyTrigger())
                .process(new UniqueVisitorWithBloomer());
        uvStream.print();

        env.execute("uv wieh bloom");

    }
    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {
        @Override
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

    public static class UniqueVisitorWithBloomer  extends
            ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        Jedis jedis;
        MyBloomFilter bloomFilter;
        final String uvCountMapName = "uvCount";

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop102", 6379);
            bloomFilter = new MyBloomFilter(1 << 29);
        }

        @Override
        public void process(ProcessAllWindowFunction<UserBehavior, PageViewCount, TimeWindow>.Context context, Iterable<UserBehavior> elements, Collector<PageViewCount> collector) throws Exception {
            Long windowEnd = context.window().getEnd();
            String bitmapKey = String.valueOf(windowEnd);
            String uvCountKey = String.valueOf(windowEnd);
            String userId = elements.iterator().next().getUserId().toString();
            Long offset = bloomFilter.hasCode(userId, 61);
            Boolean isExist = jedis.getbit(bitmapKey, offset);
            if(!isExist){
                jedis.setbit(bitmapKey, offset, true);
                Long uvCount = 0L;
                String uvCountStr = jedis.hget(uvCountMapName, uvCountKey);
                if (uvCountStr != null && !"".equals(uvCountStr)) {
                    uvCount = Long.valueOf(uvCountStr);
                }
                jedis.hset(uvCountMapName, uvCountKey, String.valueOf(uvCount
                        + 1));
            }


        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }

    }

    public static class MyBloomFilter{
        private Integer cap;
        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }
        public Long hasCode(String value, Integer seed) {
            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result * seed + value.charAt(i);
            }
            return result & (cap - 1);
        }
    }

}
