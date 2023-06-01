package com.pioneer.learn.ridecleansing;

import com.pioneer.learn.common.model.TaxiRide;
import com.pioneer.learn.common.sources.TaxiRideGenerator;
import com.pioneer.learn.utils.GeoUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * The task of this exercise is to filter a data stream of taxi ride records to keep only rides
 * that both start and end within New York City. The resulting stream should be printed.
 */
public class RideCleansingExercise {
    private final SourceFunction<TaxiRide> source;

    private final SinkFunction<TaxiRide> sink;

    public RideCleansingExercise(SourceFunction<TaxiRide> source, SinkFunction<TaxiRide> sink) {
        this.source = source;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(source).filter(new NYCFilter()).addSink(sink);

        return env.execute("Taxi Ride Cleansing");
    }

    /**
     * Keep only those rides and both start and end in NYC.
     */
    public static class NYCFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat)
                    && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }

    public static void main(String[] args) throws Exception {
        RideCleansingExercise exercise = new RideCleansingExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());
        exercise.execute();
    }
}
