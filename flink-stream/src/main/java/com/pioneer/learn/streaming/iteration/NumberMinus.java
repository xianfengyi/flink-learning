package com.pioneer.learn.streaming.iteration;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NumberMinus {
    public static void  main(String[]args) throws Exception {
        StreamExecutionEnvironment  env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Long> someIntegers = env.generateSequence(0, 2);

        someIntegers.print();

        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1 ;
            }
        });
        minusOne.print();

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        //stillGreaterThanZero.print();

        iteration.closeWith(stillGreaterThanZero);
        //
        //DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
        //    @Override
        //    public boolean filter(Long value) throws Exception {
        //        return (value <= 0);
        //    }
        //});
        //System.out.println("lessThanZero result:\n");
        //lessThanZero.print();

        env.execute();
    }
}
