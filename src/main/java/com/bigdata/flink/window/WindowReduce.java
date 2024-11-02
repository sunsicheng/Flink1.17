package com.bigdata.flink.window;

import com.bigdata.flink.base.BaseClass;
import com.bigdata.flink.pojo.WaterSensor;
import com.bigdata.flink.transform.MyMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: hadoop
 * @create-date: 2024/11/2 16:48
 */

public class WindowReduce extends BaseClass {
    public static void main(String[] args) throws Exception {
        new WindowReduce().start();
    }

    @Override
    protected void run(StreamExecutionEnvironment env) {
        DataStreamSource<String> sour = env.socketTextStream("127.0.0.1", 888);
        KeyedStream<WaterSensor, String> keyedStream = sour.map(new MyMapFunction()).keyBy(x -> x.getId());
        SingleOutputStreamOperator<WaterSensor> reduceStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10))).reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
            }
        });
        reduceStream.print();
    }
}
