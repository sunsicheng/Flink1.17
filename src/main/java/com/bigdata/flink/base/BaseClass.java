package com.bigdata.flink.base;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author: hadoop
 * @create-date: 2024/10/1 14:39
 */

public  abstract class BaseClass {

    public void start() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT,999);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        run(env);
        env.execute();
    }

    protected abstract void run(StreamExecutionEnvironment env);

}
