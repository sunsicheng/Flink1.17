package com.bigdata.flink.transform;

import com.bigdata.flink.base.BaseClass;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: hadoop
 * @create-date: 2024/10/25 10:29
 */

// 实现类似流join的功能，来一条数据，需要做两件事，1.缓存这条数据，2.去对方缓存中找是否有此id的数据
public class ConnectJoin extends BaseClass {
    public static void main(String[] args) throws Exception {
        new ConnectJoin().start();
    }

    @Override
    protected void run(StreamExecutionEnvironment env) {
        //如果并行度不是1的话，需要使用keyby让同一个key的数据发到一个task上
        env.setParallelism(1);
        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromCollection(Arrays.asList(Tuple2.of(1, "zhangsna"), Tuple2.of(2, "lisi"), Tuple2.of(3, "wangwu")));
        DataStreamSource<Tuple3<Integer, String, String>> source2 =
                env.fromCollection(Arrays.asList(Tuple3.of(1, "a", "flink"),
                        Tuple3.of(2, "b", "spark"),
                        Tuple3.of(3, "c", "hive"),
                        Tuple3.of(3, "d", "hadoop")
                ));

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, String>> connectStream = source1.connect(source2);
        //如果并行度不是1的话，需要使用keyby让同一个key的数据发到一个task上
//        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, String>> keyedStream = connectStream.keyBy(1, 1);
        connectStream.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>, Object>() {
            Map<Integer, ArrayList<Tuple3<Integer, String, String>>> cache1 = new HashMap<Integer, ArrayList<Tuple3<Integer, String, String>>>();
            Map<Integer, ArrayList<Tuple2<Integer, String>>> cache2 = new HashMap<Integer, ArrayList<Tuple2<Integer, String>>>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>, Object>.Context ctx, Collector<Object> out) throws Exception {
                //1. 如果缓存中不存在，缓存自己
                if (!cache2.containsKey(value.f0)) {
                    //ArrayList<Tuple2<Integer, String>> tuple2s = cache2.get(value.f0);
                    ArrayList<Tuple2<Integer, String>> list = new ArrayList<>();
                    list.add(value);
                    cache2.put(value.f0, list);
                }

                //2.去对方缓存中查找
                if (cache1.containsKey(value.f0)) {
                    ArrayList<Tuple3<Integer, String, String>> list = cache1.get(value.f0);
                    for (Tuple3<Integer, String, String> item : list) {
                        out.collect(item.f0 + "--" + item.f1 + "--" + item.f2 + "--" + value.f1);
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, String>, Object>.Context ctx, Collector<Object> out) throws Exception {
                //1. 如果缓存中不存在，缓存自己
                if (!cache1.containsKey(value.f0)) {
                    //ArrayList<Tuple3<Integer, String,String>> tuple2s = cache1.get(value.f0);
                    ArrayList<Tuple3<Integer, String, String>> list = new ArrayList<>();
                    list.add(value);
                    cache1.put(value.f0, list);
                }

                //2.去对方缓存中查找
                if (cache2.containsKey(value.f0)) {
                    ArrayList<Tuple2<Integer, String>> list = cache2.get(value.f0);
                    for (Tuple2<Integer, String> item : list) {
                        out.collect(value.f0 + "--" + value.f1 + "--" + value.f2 + "--" + item.f1);
                    }
                }
            }
        }).print();

    }
}
