package com.bigdata.flink.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author: hadoop
 * @create-date: 2024/10/1 14:39
 */

public  abstract class BaseClass {

    public void execut() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        run(env);
        env.execute();
    }

    protected abstract void run(StreamExecutionEnvironment env);

}
