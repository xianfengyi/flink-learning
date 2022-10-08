package com.pioneer.learn.worldcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 滑动窗口计算
 * <p>
 * 通过socket模拟产生单词数据, flink对数据进行统计计算
 * <p>
 * 需要实现每隔1秒对最近2秒内的数据进行汇总计算
 * <p>
 * <p>
 */
public class SocketWindowWordCountJava {

    public static void main(String[] args) throws Exception {
        //获取需要的端口号
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. use default port 9000--java");
            port = 9000;
        }

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "localhost";
        String delimiter = "\n";
        //连接socket获取输入的数据
        DataStream<String> text = env.socketTextStream(hostname, port, delimiter);

        DataStream<Tuple2<String, Integer>> windowCounts =
                text.flatMap(new WordCountFlatMap()).keyBy(new WordCountKeySelector()).timeWindow(Time.seconds(2),
                                Time.seconds(1))//指定时间窗口大小为2秒，指定时间间隔为1秒
                .sum("count");
        //把数据打印到控制台并且设置并行度
        windowCounts.print().setParallelism(1);

        //这一行代码一定要实现，否则程序不执行
        env.execute("Socket window count");
    }

    public static class WordCountKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            return stringIntegerTuple2.f0;
        }
    }

    public static class WordCountFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            // 遍历所有word，包成二元组输出
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
