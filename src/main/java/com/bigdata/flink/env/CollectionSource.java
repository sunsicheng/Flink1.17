package com.bigdata.flink.env;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author: hadoop
 * @create-date: 2024/9/27 14:59
 */

public class CollectionSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(Arrays.asList("hello","worle","english","chinese")).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return  "hadoop__"+ value;
            }
        }).print();
        env.execute();
    }
}
