package com.pioneer.learn.hourlytips;

import com.pioneer.learn.common.model.TaxiFare;
import com.pioneer.learn.common.sources.TaxiFareGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise {

    private SourceFunction<TaxiFare> source;

    private SinkFunction<Tuple3<Long, Long, Float>> sink;

    public HourlyTipsExercise(SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {
        this.source = source;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(source).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TaxiFare>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis()));

        // compute tips per hour for each driver
        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares.keyBy(new TaxiFareKeySelect())
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new AggregateTips());

        // find the driver with the highest sum of tips for each hour
        DataStream<Tuple3<Long, Long, Float>> hourlyMax =
                hourlyTips.windowAll(TumblingEventTimeWindows.of(Time.hours(1))).maxBy(2);

        hourlyMax.addSink(sink);

        // execute the transformation pipeline
        return env.execute("Hourly Tips");
    }

    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job = new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    public static class TaxiFareKeySelect implements KeySelector<TaxiFare, Long> {
        @Override
        public Long getKey(TaxiFare taxiFare) throws Exception {
            return taxiFare.driverId;
        }
    }

    public static class AggregateTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long,
            TimeWindow> {
        @Override
        public void process(Long key,
                            ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>.Context context,
                            Iterable<TaxiFare> fares,
                            Collector<Tuple3<Long, Long, Float>> collector) throws Exception {
            float sumOfTips = 0F;
            for (TaxiFare f : fares) {
                sumOfTips += f.tip;
            }
            collector.collect(Tuple3.of(context.window().getEnd(), key, sumOfTips));
        }
    }
}
