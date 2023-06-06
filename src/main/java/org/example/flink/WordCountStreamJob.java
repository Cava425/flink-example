package org.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountStreamJob {

    //private static final Pattern pattern = Pattern.compile("\\b\\w+\\b");
    private static final Pattern pattern = Pattern.compile("[\\w']+");

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 打开终端，运行 nc -lp 9999
         *
         * 向集群提交作业
         * 1. mvn clean package
         * 2.使用flink客户端工具提交作业
         * $ bin/flink run --class org.example.flink.WordCountStreamJob D:/AppData/Projects/GitHub/flink-example/target/flink-example-1.0-SNAPSHOT.jar
         */
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> wordCount = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {

                Matcher matcher = pattern.matcher(line);

                while (matcher.find()){
                    String token = matcher.group();
                    if (token.length() > 0) {
                        collector.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // Sink
        wordCount.print();

        env.execute("Word Count Job");
    }

}
