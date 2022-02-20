package com.atguigu.networkflow_analysis;

import com.atguigu.networkflow_analysis.beans.PageViewCount;
import com.atguigu.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class UniqueVisitor {
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
                .apply(new UvCountResult());
        uvStream.print();
        env.execute("Uv Count job");

    }

    public static class UvCountResult implements
            AllWindowFunction<UserBehavior, PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow timeWindow, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            Set<Long> idSet = new HashSet<>();
            for (UserBehavior ub : iterable) {
                idSet.add(ub.getUserId());
            }
            collector.collect(new PageViewCount("uv", timeWindow.getEnd(),
                    (long) idSet.size()));

        }
    }
}
