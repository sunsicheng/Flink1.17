package com.bigdata.flink.transform;

import com.bigdata.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author: hadoop
 * @create-date: 2024/10/1 10:35
 */

public class AggDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> sour = env.fromCollection(Arrays.asList(
                new WaterSensor("1001", 123L, 10),
                new WaterSensor("1002", 236L, 5),
                new WaterSensor("1003", 187L, 9),
                new WaterSensor("1001", 235L, 15)
        ));


        KeyedStream<WaterSensor, String> keyedStream = sour.keyBy(x -> x.id);
        keyedStream.sum("vc").print();

        /*** 输出结果
         *    同组其他数据，sum,max,min等会取第一条，minby，maxby等会取对应的最大或者最小的数据的字段
         * 6> WaterSensor{id='1001', ts=123, vc=10}
         * 7> WaterSensor{id='1003', ts=187, vc=9}
         * 6> WaterSensor{id='1002', ts=236, vc=5}
         * 6> WaterSensor{id='1001', ts=123, vc=25}
         */

        env.execute();
    }
}
