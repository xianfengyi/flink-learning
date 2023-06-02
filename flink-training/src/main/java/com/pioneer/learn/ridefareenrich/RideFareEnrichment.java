package com.pioneer.learn.ridefareenrich;

import com.pioneer.learn.common.model.RideAndFare;
import com.pioneer.learn.common.model.TaxiFare;
import com.pioneer.learn.common.model.TaxiRide;
import com.pioneer.learn.common.sources.TaxiFareGenerator;
import com.pioneer.learn.common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * The goal for this exercise is to enrich TaxiRides with fare information.
 */
public class RideFareEnrichment {
    private final SourceFunction<TaxiRide> rideSource;
    private final SourceFunction<TaxiFare> fareSource;
    private final SinkFunction<RideAndFare> sink;

    public RideFareEnrichment(SourceFunction<TaxiRide> rideSource, SourceFunction<TaxiFare> fareSource,
                              SinkFunction<RideAndFare> sink) {
        this.rideSource = rideSource;
        this.fareSource = fareSource;
        this.sink = sink;
    }

    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return execute(env);
    }

    public JobExecutionResult execute(StreamExecutionEnvironment env) throws Exception {
        // A stream of taxi ride START events, keyed by rideId.
        DataStream<TaxiRide> rides =
                env.addSource(rideSource).filter(ride -> ride.isStart).keyBy(ride -> ride.rideId);

        // A stream of taxi fare events, also keyed by rideId.
        DataStream<TaxiFare> fares = env.addSource(fareSource).keyBy(fare -> fare.rideId);

        // Create the pipeline.
        rides.connect(fares)
                .flatMap(new EnrichmentFunction())
                .uid("enrichment") // uid for this operator's state
                .name("enrichment") // name for this operator in the web UI
                .addSink(sink);

        // Execute the pipeline and return the result.
        return env.execute("Join Rides with Fares");
    }

    public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> {
        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            rideState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            fareState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide taxiRide, Collector<RideAndFare> collector) throws Exception {
            TaxiFare fare = fareState.value();
            if (fare != null) {
                fareState.clear();
                collector.collect(new RideAndFare(taxiRide, fare));
            } else {
                rideState.update(taxiRide);
            }
        }

        @Override
        public void flatMap2(TaxiFare taxiFare, Collector<RideAndFare> collector) throws Exception {
            TaxiRide ride = rideState.value();
            if (ride != null) {
                rideState.clear();
                collector.collect(new RideAndFare(ride, taxiFare));
            } else {
                fareState.update(taxiFare);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        RideFareEnrichment job =
                new RideFareEnrichment(new TaxiRideGenerator(), new TaxiFareGenerator(), new PrintSinkFunction<>());

        // Setting up checkpointing so that the state can be explored with the State Processor API.
        // Generally it's better to separate configuration settings from the code,
        // but for this example it's convenient to have it here for running in the IDE.
        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
        conf.setString("execution.checkpointing.interval", "10s");
        conf.setString(
                "execution.checkpointing.externalized-checkpoint-retention",
                "RETAIN_ON_CANCELLATION");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        job.execute(env);
    }

}
