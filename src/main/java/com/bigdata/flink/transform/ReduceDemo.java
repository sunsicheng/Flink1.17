package com.bigdata.flink.transform;

import com.bigdata.flink.base.BaseClass;
import com.bigdata.flink.pojo.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author: hadoop
 * @create-date: 2024/10/1 14:38
 */

public class ReduceDemo extends BaseClass {

    public static void main(String[] args) throws Exception {
        new ReduceDemo().execut();
    }

    @Override
    protected void run(StreamExecutionEnvironment env) {
        DataStreamSource<WaterSensor> ds = env.fromCollection(Arrays.asList(
                new WaterSensor("1001", 123L, 10),
                new WaterSensor("1002", 236L, 5),
                new WaterSensor("1003", 187L, 9),
                new WaterSensor("1001", 235L, 15)));

//        ds.keyBy(x->x.id).reduce(new ReduceFunction<WaterSensor>() {
//            @Override
//            /***重要：同一个key的累计值会当做下一个的value1，新值被当做value2 ***/
//            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
//                System.out.println("value1="+value1);
//                System.out.println("value2="+value2);
//                return new WaterSensor(value1.id,value1.ts,value1.vc+value2.vc);
//            }
//        }).print();


        ds.keyBy(x->x.id).reduce((x1,x2)-> new WaterSensor(x1.id,x1.ts,x1.vc+x2.vc)).print();
    }
}
