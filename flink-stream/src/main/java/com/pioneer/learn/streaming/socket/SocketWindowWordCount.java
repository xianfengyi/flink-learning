package com.pioneer.learn.streaming.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
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
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {
        String hostname = "localhost";
        int port = 9093;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. use default host name and port");
        }

        //获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接socket获取输入的数据
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        DataStream<WordWithCount> windowCounts =
                text.flatMap(new WordCountFlatMap())
                        .keyBy(new WordCountKeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .reduce((a, b) -> new WordWithCount(a.word, a.count + b.count))
                        .returns(WordWithCount.class);
        //把数据打印到控制台并且设置并行度
        windowCounts.print().setParallelism(1);

        //这一行代码一定要实现，否则程序不执行
        env.execute("Socket window count");
    }

    public static class WordCountKeySelector implements KeySelector<WordWithCount, String> {
        @Override
        public String getKey(WordWithCount wordWithCount) throws Exception {
            return wordWithCount.word;
        }
    }

    public static class WordCountFlatMap implements FlatMapFunction<String, WordWithCount> {
        @Override
        public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
            for (String word : value.split("\\s")) {
                out.collect(new WordWithCount(word, 1L));
            }
        }
    }

    /**
     * Data type for words with count.
     */
    public static class WordWithCount {

        public String word;
        public long count;

        @SuppressWarnings("unused")
        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
