package com.bigdata.flink.partition;

import com.alibaba.fastjson.JSON;
import com.bigdata.flink.base.BaseClass;
import com.bigdata.flink.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: hadoop
 * @create-date: 2024/10/2 19:46
 */

public class CustomerPartitionDemo extends BaseClass {

    public static void main(String[] args) throws Exception {
        new CustomerPartitionDemo().start();
    }

    @Override
    protected void run(StreamExecutionEnvironment env) {
        DataStreamSource<String> ds = env.socketTextStream("localhost", 888);
        ds.map(x -> JSON.parseObject(x, WaterSensor.class))
                .partitionCustom(new MyPartitioner(),x->x.getId())
                .print().setParallelism(3);

    }
}
