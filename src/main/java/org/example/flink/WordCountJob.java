package org.example.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.Arrays;

public class WordCountJob {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = env.fromCollection(Arrays.asList("Hello", "To be,or not to be:that is the question:"));

        dataSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        }).print();
    }

}
