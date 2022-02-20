package com.atguigu.networkflow_analysis;

import com.atguigu.networkflow_analysis.beans.ApacheLogEvent;
import com.atguigu.networkflow_analysis.beans.PageViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.jute.CsvOutputArchive;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

public class HotPages {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = HotPages.class.getResource("/apache.log");
        DataStream<String> inputStream = env.readTextFile("C:\\Users\\Junchi Lin\\IdeaProjects\\UserBehaviorAnalysis\\Hotpages\\src\\main\\resources\\apache.log");
        DataStream<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();

            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<ApacheLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<ApacheLogEvent>)
                        (element, recordTimeStamp) -> element.getTimestamp() * 1));
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late") {
        };
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream
                .filter(data -> "GET".equals(data.getMethod()))
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(item -> item.getUrl())
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());
        windowAggStream.getSideOutput(lateTag).print();

        DataStream<String> resultStream = windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));
        resultStream.print("Hot Page");
        env.execute("hot page");
    }

    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(url, timeWindow.getEnd(),
                    iterable.iterator().next()));
        }
    }

    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        //        ListState<PageViewCount> pageViewCountMapState;
        MapState<String, Long> pageViewCountMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
//            pageViewCountMapState = getRuntimeContext().getListState(new
//                    ListStateDescriptor<PageViewCount>("page-count-map", PageViewCount.class));
            pageViewCountMapState = getRuntimeContext().getMapState(new
                    MapStateDescriptor<String, Long>("page-count-map", String.class,
                    Long.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, KeyedProcessFunction<Long, PageViewCount, String>.Context ctx, Collector<String> collector) throws Exception {
//            pageViewCountMapState.add(pageViewCount);
            pageViewCountMapState.put(pageViewCount.getUrl(), pageViewCount.getCount());
            ctx.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() +
                    1);
            ctx.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() +
                    60 * 1000);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, PageViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
//            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountMapState.get());
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries());
            if (timestamp == ctx.getCurrentKey() + 60 * 1000L) {
                pageViewCountMapState.clear();
                return;
            }



            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1,
                                   Map.Entry<String, Long> o2) {
                    if (o1.getValue() > o2.getValue())
                        return -1;
                    else if (o1.getValue() < o2.getValue())
                        return 1;
                    else
                        return 0;
                }
            });

            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("==============\n");

            resultBuilder.append("Endtime：").append(new Timestamp(timestamp - 1)).append("\n");


            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i + 1).append(":")
                        .append(" URL = ").append(currentItemViewCount.getKey())
                        .append(" Page View = ").append(currentItemViewCount.getValue())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");


            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
            pageViewCountMapState.clear();
        }
    }
}
