package com.pioneer.learn.streaming.join;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Example illustrating a windowed stream join between two data streams.
 *
 * <p>The example works on two input streams with pairs (name, grade) and (name, salary)
 * respectively. It joins the streams based on "name" within a configurable window.
 *
 * <p>The example uses a built-in sample data generator that generates the streams of pairs at a
 * configurable rate.
 *
 * <p>
 * To customize example, use:
 * <pre>
 *     WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]
 * </pre>
 */
public class WindowJoin {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 2000);
        final long rate = params.getLong("rate", 3L);

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple2<String, Integer>> grades = SampleData.GradeSource.getSource(env, rate)
                .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Tuple2<String, Integer>> salaries = SampleData.SalarySource.getSource(env, rate)
                .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        // run the actual window join program
        // for testability, this functionality is in a separate method.
        DataStream<Tuple3<String, Integer, Integer>> joinedStream = runWindowJoin(grades, salaries, windowSize);

        // print the results with a single thread, rather than in parallel
        joinedStream.print().setParallelism(1);

        // execute program
        env.execute("Windowed Join Example");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowJoin(
            DataStream<Tuple2<String, Integer>> grades, DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {
        return grades.join(salaries)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(getJoinFunction());
    }

    private static JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>> getJoinFunction() {
        return new JoinFunction<>() {
            @Override
            public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first,
                                                         Tuple2<String, Integer> second) {
                return new Tuple3<>(first.f0, first.f1, second.f1);
            }
        };
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    /**
     * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
     * In a real use case you should use proper timestamps and an appropriate {@link
     * WatermarkStrategy}.
     */
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}
