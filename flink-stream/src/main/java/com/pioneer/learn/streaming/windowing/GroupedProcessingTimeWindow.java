package com.pioneer.learn.streaming.windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class GroupedProcessingTimeWindow {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Long, Long>> stream = env.addSource(new MyDataSource());
        stream.keyBy(value -> value.f0)
                .window(SlidingProcessingTimeWindows.of(Time.milliseconds(2500), Time.milliseconds(500)))
                .reduce(new SummingReducer())
                .addSink(new DiscardingSink<>());
        env.execute();
    }

    private static class FirstFieldKeySelector<IN extends Tuple, Key> implements KeySelector<IN, Key> {
        @Override
        public Key getKey(IN value) throws Exception {
            return (Key) value.getField(0);
        }
    }

    private static class SummingWindowFunction implements WindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long
            , Window> {
        @Override
        public void apply(Long key, Window window, Iterable<Tuple2<Long, Long>> iterable, Collector<Tuple2<Long,
                Long>> collector) throws Exception {
            long sum = 0L;
            for (Tuple2<Long, Long> value : iterable) {
                sum += value.f1;
            }
            collector.collect(new Tuple2<>(key, sum));
        }
    }

    private static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }

    private static class MyDataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
            final long startTime = System.currentTimeMillis();
            final long numElements = 20000000;
            final long numKeys = 10000;
            long val = 1L;
            long count = 0L;
            while (running && count < numKeys) {
                count++;
                sourceContext.collect(new Tuple2<>(val++, 1L));
                if (val > numKeys) {
                    val = 1L;
                }
            }
            final long endTime = System.currentTimeMillis();
            System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
