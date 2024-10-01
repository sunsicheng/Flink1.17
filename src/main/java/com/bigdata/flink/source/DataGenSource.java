package com.bigdata.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author: hadoop
 * @create-date: 2024/9/29 16:22
 */

public class DataGenSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataGeneratorSource<String> source = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "hello" + value;
            }
        },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(100),
                TypeInformation.of(String.class)
        );
        env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "datagen-source")
                .print();
        env.execute();
    }
}
