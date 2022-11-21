package com.pioneer.learn.streaming.windowing;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SessionWindowing {
    public static void main(String[] args) throws Exception {
        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, Long, Integer>> source =
                env.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
                    @Override
                    public void run(SourceContext<Tuple3<String, Long, Integer>> sourceContext) throws Exception {
                        for (Tuple3<String, Long, Integer> value : getInput()) {
                            sourceContext.collectWithTimestamp(value, value.f1);
                            sourceContext.emitWatermark(new Watermark(value.f1 - 1));
                        }
                        sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));
                    }

                    @Override
                    public void cancel() {

                    }
                });

        // We create sessions for each id with max timeout of 3 time units
        DataStream<Tuple3<String, Long, Integer>> aggregated = source.keyBy(value -> value.f0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
                .sum(2);

        aggregated.print();

        env.execute();
    }


    private static List<Tuple3<String, Long, Integer>> getInput() {
        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

        input.add(new Tuple3<>("a", 1L, 1));
        input.add(new Tuple3<>("b", 1L, 1));
        input.add(new Tuple3<>("b", 3L, 1));
        input.add(new Tuple3<>("b", 5L, 1));
        input.add(new Tuple3<>("c", 6L, 1));
        // We expect to detect the session "a" earlier than this point (the old
        // functionality can only detect here when the next starts)
        input.add(new Tuple3<>("a", 10L, 1));
        // We expect to detect session "b" and "c" at this point as well
        input.add(new Tuple3<>("c", 11L, 1));
        return input;
    }
}
