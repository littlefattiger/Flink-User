package com.atguigu.networkflow_analysis;

import com.atguigu.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.util.Random;

public class PageView {
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

        SingleOutputStreamOperator<Tuple2<String, Long>> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    Random random = new Random();
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1) ))
                .sum(1);
        windowAggStream.print();
        env.execute("pv count job");

    }
}
