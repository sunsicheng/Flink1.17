package com.bigdata.flink.transform;

import com.bigdata.flink.base.BaseClass;
import com.bigdata.flink.pojo.Person;
import com.bigdata.flink.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author: hadoop
 * @create-date: 2024/10/3 10:32
 */


/**
 * TODO对数据进行分流，如果id是s1的则输出到s1侧输出流，id是s2的则输出到s2侧输出流，其他数据则输出到主流
 */
public class SideOutPut extends BaseClass {


    public static void main(String[] args) throws Exception {
        new SideOutPut().execut();
    }

    @Override
    protected void run(StreamExecutionEnvironment env) {
        List<String> placesList = Arrays.asList(
                "北京市", "上海市", "广州市", "深圳市"
        );

        Random random = new Random();
        //定义datagen

        DataGeneratorSource<Person> dataGenSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, Person>() {
                    @Override
                    public Person map(Long value) throws Exception {
                        //value 就是计数器，累加的从0开始
                        String name = "Person" + value;
                        int age = (int) (value % 100);  // 生成随机年龄（假设最多 100 岁）

                        String randomPlace = placesList.get(random.nextInt(placesList.size()));
                        return new Person(name, age, randomPlace);

                    }
                },
                //Long.MAX_VALUE,
                100,
                RateLimiterStrategy.perSecond(5),
                Types.POJO(Person.class)
        );

        //读取数据源
        DataStreamSource<Person> source = env.fromSource(
                dataGenSource
                , WatermarkStrategy.noWatermarks()
                , "data-gen"
        );

        //对数据进行分流操作
        OutputTag<Person> shenzhen = new OutputTag<>("shenzhen", Types.POJO(Person.class));
        OutputTag<Person> guangzhouTag = new OutputTag<>("guangzhou", Types.POJO(Person.class));
        SingleOutputStreamOperator<Person> processDS = source.process(new ProcessFunction<Person, Person>() {
            @Override
            public void processElement(Person value, ProcessFunction<Person, Person>.Context ctx, Collector<Person> out) throws Exception {
                //使用侧输出流进行分流
                if ("广州市".equals(value.getAddress())) {
                    ctx.output(guangzhouTag, value);
                } else if ("深圳市".equals(value.getAddress())) {
                    ctx.output(shenzhen, value);
                } else {
                    out.collect(value);
                }
            }
        });

        //获取侧输出流
        processDS.getSideOutput(shenzhen).printToErr("深圳");
        processDS.getSideOutput(guangzhouTag).printToErr("广州");
        processDS.print("主流");
    }
}
