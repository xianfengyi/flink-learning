package com.pioneer.learn.longrides;

import com.pioneer.learn.common.model.TaxiRide;
import com.pioneer.learn.common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 */
public class LongRidesExercise {

    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<Long> sink;

    public LongRidesExercise(SourceFunction<TaxiRide> source, SinkFunction<Long> sink) {
        this.source = source;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(source);

        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis());

        rides.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(sink);

        // execute the pipeline and return the result
        return env.execute("Long Taxi Rides");
    }

    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {
        private ValueState<TaxiRide> rideState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<TaxiRide> rideStateDescriptor = new ValueStateDescriptor<>("ride event",
                    TaxiRide.class);
            rideState = getRuntimeContext().getState(rideStateDescriptor);
        }

        @Override
        public void processElement(TaxiRide taxiRide, KeyedProcessFunction<Long, TaxiRide, Long>.Context context,
                                   Collector<Long> collector) throws Exception {
            TaxiRide firstRideEvent = rideState.value();

            if (firstRideEvent == null) {
                // whatever event comes first, remember it
                rideState.update(taxiRide);
                if (taxiRide.isStart) {
                    // we will use this timer to check for rides that have gone on too long and may
                    // not yet have an END event (or the END event could be missing)
                    context.timerService().registerEventTimeTimer(getTimerTime(taxiRide));
                }
            } else {
                if (taxiRide.isStart) {
                    if (isRideTooLong(taxiRide, firstRideEvent)) {
                        collector.collect(taxiRide.rideId);
                    }
                } else {
                    // the first ride was a START event, so there is a timer unless it has fired
                    context.timerService().deleteEventTimeTimer(getTimerTime(firstRideEvent));

                    // perhaps the ride has gone on too long, but the timer didn't fire yet
                    if (isRideTooLong(firstRideEvent, taxiRide)) {
                        collector.collect(taxiRide.rideId);
                    }
                }

                // both events have now been seen, we can clear the state
                // this solution can leak state if an event is missing
                // see DISCUSSION.md for more information
                rideState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out) throws Exception {
            // the timer only fires if the ride was too long
            out.collect(rideState.value().rideId);

            // clearing now prevents duplicate alerts, but will leak state if the END arrives
            rideState.clear();
        }

        private boolean isRideTooLong(TaxiRide startEvent, TaxiRide endEvent) {
            return Duration.between(startEvent.eventTime, endEvent.eventTime).compareTo(Duration.ofHours(2)) > 0;
        }

        private long getTimerTime(TaxiRide ride) throws RuntimeException {
            if (ride.isStart) {
                return ride.eventTime.plusSeconds(120 * 60).toEpochMilli();
            } else {
                throw new RuntimeException("Can not get start time from END event.");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        LongRidesExercise job = new LongRidesExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }
}
