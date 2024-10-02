package com.bigdata.flink.pojo;

import com.alibaba.fastjson.JSONObject;

import java.util.Objects;

/**
 * @author: hadoop
 * @create-date: 2024/9/27 14:55
 */

public class WaterSensor {
    public String id;
    public Long ts;


    public Integer vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }


    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(id, that.id) && Objects.equals(ts, that.ts) && Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, vc);
    }

    public static void main(String[] args) {
        WaterSensor waterSensor = new WaterSensor("1001", 1692345L, 123);
        System.out.println(waterSensor.toString());
        System.out.println(JSONObject.toJSONString(waterSensor));
        System.out.println("1001".hashCode());
        System.out.println("1002".hashCode());
        System.out.println("1003".hashCode());
        System.out.println("1004".hashCode());
    }
}
