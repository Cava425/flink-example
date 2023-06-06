package org.example.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.example.flink.source.CustomStringSource;
import org.junit.Test;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCountStreamTests {

    @Test
    public void filterTest() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> streamSource = env.fromCollection(Arrays.asList("Hello Java", "Hello Flink", "Hello World"));

        streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.endsWith("Flink");
            }
        }).print();

        env.execute("Flink Example");
    }


    @Test
    public void flatMapTest() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> streamSource = env.fromElements("Hello Java", "Hello Flink", "Hello World", "I`m man");

        final Pattern pattern = Pattern.compile("[\\w`]+");

        streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                Matcher matcher = pattern.matcher(s);
                while (matcher.find()){
                    collector.collect(matcher.group());
                }
            }
        }).returns(Types.STRING).print();

        env.execute("Flink Example");
    }



    @Test
    public void keyByTest() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> streamSource = env.fromElements(
                Tuple2.of("ZhangSan", 69),
                Tuple2.of("LiSi", 99),
                Tuple2.of("ZhangSan", 78),
                Tuple2.of("LiSi", 87),
                Tuple2.of("WangWu", 32)
        );

        streamSource.keyBy(0).sum(1).print();

        env.execute("Flink Example");
    }

    @Test
    public void keyByTest2() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> streamSource = env.fromElements(
                Tuple2.of("ZhangSan", 69),
                Tuple2.of("LiSi", 99),
                Tuple2.of("ZhangSan", 78),
                Tuple2.of("LiSi", 87),
                Tuple2.of("WangWu", 32)
        );

        streamSource.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).sum(1).print();

        env.execute("Flink Example");
    }


    @Test
    public void reduceTest() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> streamSource = env.fromElements(
                Tuple2.of("ZhangSan", 69),
                Tuple2.of("LiSi", 99),
                Tuple2.of("ZhangSan", 78),
                Tuple2.of("LiSi", 87),
                Tuple2.of("WangWu", 32)
        );

        streamSource.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        }).print();

        env.execute("Flink Example");
    }


    /**
     * 多数据流转换
     * 1.union      合并多个数据流（数据流的类型必须一致），数据流按照先进先出的模式合并，且不去重
     * 2.connect    连接两个数据流
     *              connect经常应用于使用一个控制流对另一个数据流控制的场景，控制流可以是阈值、规则、机器学习模型或其他参数
     */
    @Test
    public void multiStreamTest() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 模拟数据流，定时生成一个字符串
        DataStreamSource<String> streamSource = env.addSource(new CustomStringSource());

        // 定义一个MapStateDescriptor描述广播的数据格式
        final MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(String.class));

        // 模拟配置流，监听控制台输出，模拟配置变更
        BroadcastStream<String> broadcastStream = env.socketTextStream("localhost", 9999).broadcast(mapStateDescriptor);

        // 数据流连接上控制流
        streamSource.connect(broadcastStream).process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                if(broadcastState.contains("config")){
                    out.collect(value + ", config=" + broadcastState.get("config"));
                }else {
                    out.collect(value);
                }
            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                broadcastState.put("config", value);
            }
        }).print();

        env.execute("Flink Example");
    }
}
