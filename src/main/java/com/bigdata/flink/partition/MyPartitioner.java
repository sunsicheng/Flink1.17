package com.bigdata.flink.partition;

import com.bigdata.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author: hadoop
 * @create-date: 2024/10/2 19:56
 */

public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {

        return Math.abs(key.hashCode()) % numPartitions;
    }

}
