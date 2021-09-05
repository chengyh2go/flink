package com.sinobridge.flink.flinksql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSqlKafkaToGPSample {
    public static void main(String[] args) {

        /*StreamExecutionEnvironment ssEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings ssSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment ssTableEnv = StreamTableEnvironment.create(ssEnv, ssSettings);*/

        EnvironmentSettings sSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment sTableEnv = TableEnvironment.create(sSettings);

        //组装source表建表语句
        sTableEnv.executeSql("" +
                "create table fission_group_source(\n" +
                "`id` bigint,\n" +
                "`group_code` string\n" +
                //"offset bigint metadata virtual\n" +
                ") with (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'fission_group',\n" +
                "'properties.bootstrap.servers' = '192.168.250.11:9092',\n" +
                "'properties.group.id' = 'fg_test',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'json.fail-on-missing-field' = 'false',\n" +
                "'json.ignore-parse-errors' = 'true',\n" +
                "'format' = 'json'\n" +
                ")");

        /*sTableEnv.executeSql("" +
                "create table fission_group_source(\n" +
                "id bigint,\n" +
                "group_code string,\n" +
                "offset as offset_value bigint METADATA VIRTUAL\n" +
                ") with (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'fission_group',\n" +
                "'properties.bootstrap.servers' = '192.168.250.11:9092',\n" +
                "'properties.group.id' = 'fg_test',\n" +
                "'scan.startup.mode' = 'earliest-offset',\n" +
                "'json.fail-on-missing-field' = 'false',\n" +
                "'json.ignore-parse-errors' = 'true',\n" +
                "'value.format' = 'debezium-json',\n" +
                "'format' = 'json'\n" +
                ")");*/


        //组装sink表的建表语句
        sTableEnv.executeSql("" +
                "create table fission_group(\n" +
                "id bigint,\n" +
                "group_code string\n" +
                ") with (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:postgresql://192.168.250.31:5432/act',\n" +
                "'username' = 'gpadmin',\n" +
                "'password' = 'root123',\n" +
                "'table-name' = 'fission_group'\n" +
                ")");

        /*sTableEnv.executeSql("" +
                "create table fission_group(\n" +
                "id bigint,\n" +
                "group_code string\n" +
                ") with (\n" +
                "'connector' = 'print'\n" +
                ")");*/

        //执行sql查询
        Table res = sTableEnv.sqlQuery("select id,group_code from fission_group_source");

        //保存结果
        res.executeInsert("fission_group");


    }
}
