package com.bigdata.flink.transform;

import com.bigdata.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author: hadoop
 * @create-date: 2024/11/2 17:07
 */

public class MyMapFunction implements MapFunction<String,WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] arr = value.toString().split(",");
        return new WaterSensor(arr[0],Long.parseLong(arr[1]),Integer.parseInt(arr[2]));
    }
}
