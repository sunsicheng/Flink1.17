package com.bigdata.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: hadoop
 * @create-date: 2024/10/3 11:01
 */


@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person {
    public String name;
    public  int age;
    public  String address;
}
