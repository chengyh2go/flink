package com.sinobridge.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sinobridge.flink.dbops.DbRecoveryOps;
import com.sinobridge.flink.entity.Fission;
import com.sinobridge.flink.entity.FissionGroup;
import com.sinobridge.flink.entity.FissionGroupMember;
import com.sinobridge.flink.sink.SinkToGreenplum;
import com.sinobridge.flink.sink.SinkToGreenplumCheckpoint;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.*;

/**
 * 示例数据：fission_group: {id: 1001, group_code: 'group_code_1'}
 * 示例数据：fission_group_member: {id: 1001, group_id: 'group_id_1'}
 */

public class DataStreamKafkaSinkToGPSampleCheckpoint {
    public static void main(String[] args) {

        //加载配置文件，获取全局配置参数
        InputStream is = DataStreamKafkaSinkToGPSampleCheckpoint.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //1、获得一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkpoint设置
        /*env.enableCheckpointing(15000);
        //设置模式为.EXACTLY_ONCE (这是默认值) ,还可以设置为AT_LEAST_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        try {
            StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://node01:9000/flink/checkpoints", true);
            env.setStateBackend(rocksDBStateBackend);
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        //2、加载/创建 初始化数据
        //兴证0.8版本需要
        //prop.setProperty("zookeeper.connect","192.25.105.188:2181");
        FlinkKafkaConsumer<ObjectNode> fissionGroupKafkaConsumer = null;
        FlinkKafkaConsumer<ObjectNode> fissionGroupMemberKafkaConsumer = null;

        //设置fissionGroupKafkaConsumer需要的参数
        String fissionGroupTopic = properties.getProperty("fission_group_topic");
        String fissionGroupMemberTopic = properties.getProperty("fission_group_member_topic");

        Properties prop = new Properties();
        prop.put("bootstrap.servers",properties.getProperty("bootstrap.servers"));
        prop.put("group.id", properties.getProperty("group.id"));

        fissionGroupKafkaConsumer = new FlinkKafkaConsumer<>(fissionGroupTopic,
                new JSONKeyValueDeserializationSchema(true),
                prop);

        fissionGroupMemberKafkaConsumer = new FlinkKafkaConsumer<>(fissionGroupMemberTopic,
                new JSONKeyValueDeserializationSchema(true),
                prop);


        //设置从何处开始消费消息
        //auto.offset.reset: Earliest || Latest(缺省值)
        /*
        select partition_value,max(offset_value) from fission_group group by partition_value;
        得到类似下面的结果：
        分区 | offset
        ---------------
        2   | 10210912
        0   |   186171
        1   | 20210912
        */

        //创建指定offset需要的map，格式：Map<KafkaTopicPartition, Long>
        Map<KafkaTopicPartition, Long> specificFissionGroupStartOffsets = new HashMap<>();
        Map<KafkaTopicPartition, Long> specificFissionGroupMemberStartOffsets = new HashMap<>();

        DbRecoveryOps dbRecoveryOps = new DbRecoveryOps(properties);
        try {
            System.out.println("开始获取fission_group的各个partition下的最新offset");
            HashMap<Integer, Long> fgMap = dbRecoveryOps.fetchFissionGroupPartitionAndOffset();
            System.out.println(fgMap);
            if (fgMap.isEmpty()) {
                fissionGroupKafkaConsumer.setStartFromEarliest();
            } else {
                Set<Map.Entry<Integer, Long>> entrySet = fgMap.entrySet();
                for (Map.Entry<Integer, Long> entry : entrySet) {
                    specificFissionGroupStartOffsets.put(new KafkaTopicPartition(fissionGroupTopic, entry.getKey()),entry.getValue() +1);
                }
                fissionGroupKafkaConsumer.setStartFromSpecificOffsets(specificFissionGroupStartOffsets);
            }

            System.out.println("开始获取fission_group_member的各个partition下的最新offset");
            HashMap<Integer, Long> fgmMap = dbRecoveryOps.fetchFissionGroupMemberPartitionAndOffset();
            System.out.println(fgmMap);
            if (fgmMap.isEmpty()) {
                fissionGroupMemberKafkaConsumer.setStartFromEarliest();
            } else {
                Set<Map.Entry<Integer, Long>> entrySet = fgmMap.entrySet();
                for (Map.Entry<Integer, Long> entry : entrySet) {
                    specificFissionGroupMemberStartOffsets.put(new KafkaTopicPartition(fissionGroupMemberTopic, entry.getKey()),entry.getValue()+1);
                }
                fissionGroupMemberKafkaConsumer.setStartFromSpecificOffsets(specificFissionGroupMemberStartOffsets);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }



        /*Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L);
        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L);
        specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L);
        myConsumer.setStartFromSpecificOffsets(specificStartOffsets);*/


        DataStreamSource<ObjectNode> fgStream = env.addSource(fissionGroupKafkaConsumer);
        DataStreamSource<ObjectNode> fgmStream = env.addSource(fissionGroupMemberKafkaConsumer);
        DataStream<ObjectNode> unionStream = fgStream.union(fgmStream);

        //3、指定操作数据的transformation算子
        /*kafka的JSONKeyValueDeserializationSchema反序列化后的ObjectNode消息格式：
        {"key":{},
        "value":{"id":4711421341564512009,"group_code":"test"},
        "metadata":{"offset":182162,"topic":"fission_group","partition":0}}*/

        SingleOutputStreamOperator<List<Fission>> soso = unionStream.map(new MapFunction<ObjectNode, Fission>() {
            @Override
            public Fission map(ObjectNode objectNode) {
                //解析kafka反序列化后的objectNode

                //如果包含group_node字段，则走fission_group逻辑
                if (objectNode.get("value").has("group_code") ){
                    long id = objectNode.get("value").get("id").asLong();
                    String group_code = objectNode.get("value").get("group_code").toString();
                    long offset_value = objectNode.get("metadata").get("offset").asLong();
                    int partition_value = objectNode.get("metadata").get("partition").asInt();
                    FissionGroup fissionGroup = new FissionGroup();
                    fissionGroup.setId(id);
                    fissionGroup.setGroup_code(group_code);
                    fissionGroup.setOffset_value(offset_value);
                    fissionGroup.setPartition_value(partition_value);
                    return fissionGroup;
                } else { //反之，走fission_group_member逻辑
                    long id = objectNode.get("value").get("id").asLong();
                    String group_id = objectNode.get("value").get("group_id").toString();
                    long offset_value = objectNode.get("metadata").get("offset").asLong();
                    int partition_value = objectNode.get("metadata").get("partition").asInt();
                    FissionGroupMember fissionGroupMember = new FissionGroupMember();
                    fissionGroupMember.setId(id);
                    fissionGroupMember.setGroup_id(group_id);
                    fissionGroupMember.setOffset_value(offset_value);
                    fissionGroupMember.setPartition_value(partition_value);
                    return fissionGroupMember;
                }
            }
        }).timeWindowAll(Time.seconds(Long.parseLong(properties.getProperty("timeWindow")))) //设置时间窗口
                .apply(new AllWindowFunction<Fission, List<Fission>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Fission> values, Collector<List<Fission>> out) throws Exception {
                        //System.out.println("进入时间窗口的时间：" + new Date());
                        List<Fission> list = new ArrayList<>();
                        for (Fission item : values) {
                            list.add(item);
                        }
                        System.out.println(new Date() + " 收集到的条目数：" + list.size());
                        if (list.size() > 0) {
                            out.collect(list);
                        }
                    }
                });

        //4、sink
        soso.addSink(new SinkToGreenplumCheckpoint(properties)).name("sink-to-gp");

        //5、执行execute方法
        try {
            env.execute("DataStreamKafkaSinkToGPSampleCheckpoint");
        } catch (Exception e) {
            e.printStackTrace();
        }

        /*
        //调试使用：只打印输出
        soso.print();
        try {
            env.execute("test");
        } catch (Exception e) {
            e.printStackTrace();
        }*/

    }
}
