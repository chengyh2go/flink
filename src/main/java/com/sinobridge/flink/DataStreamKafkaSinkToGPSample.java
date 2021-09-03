package com.sinobridge.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sinobridge.flink.entity.Fission;
import com.sinobridge.flink.entity.FissionGroup;
import com.sinobridge.flink.entity.FissionGroupMember;
import com.sinobridge.flink.sink.SinkToGreenplum;
import com.sinobridge.flink.sink.SinkToGreenplumMultiTransaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * 示例数据：fission_group: {id: 1001, group_code: 'group_code_1'}
 * 示例数据：fission_group_member: {id: 1001, group_id: 'group_id_1'}
 */

public class DataStreamKafkaSinkToGPSample {
    public static void main(String[] args) {
        //加载配置文件，获取全局配置参数
        InputStream is = DataStreamKafkaSinkToGPSample.class.getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //1、获得一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        //设置模式为.EXACTLY_ONCE (这是默认值) ,还可以设置为AT_LEAST_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        try {
            StateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://node01:9000/flink/checkpoints", true);
            env.setStateBackend(rocksDBStateBackend);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //2、加载/创建 初始化数据
        //兴证0.8版本需要
        //prop.setProperty("zookeeper.connect","192.25.105.188:2181");
        FlinkKafkaConsumer<String> kafkaConsumer = null;
        String topic = properties.getProperty("topic");
        if (topic.contains(",")) {
            String[] topicArr = topic.split(",");
            List<String> topicList = Arrays.asList(topicArr);
            kafkaConsumer = new FlinkKafkaConsumer<>(topicList, new SimpleStringSchema(), properties);
        } else {
            kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        }

        //设置从何处开始消费消息
        //auto.offset.reset: Earliest || Latest(缺省值)
        kafkaConsumer.setStartFromGroupOffsets();

        DataStreamSource<String> text = env.addSource(kafkaConsumer);

        //3、指定操作数据的transformation算子
        SingleOutputStreamOperator<List<Fission>> soso = text.map(new MapFunction<String, Fission>() {
            @Override
            public Fission map(String message) throws Exception {
                //解析kafka中的消息
                JSONObject jsonObject = JSON.parseObject(message);
                //如果包含group_code这个key，走FissionGroup解析逻辑
                if (jsonObject.containsKey("group_code")) {
                    return JSON.parseObject(message, FissionGroup.class);
                }
                //否则走FissionGroupMember解析逻辑
                return JSON.parseObject(message, FissionGroupMember.class);
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

        /*
        //使用druid创建连接池
        String driverName = properties.getProperty("driverName");
        String url = properties.getProperty("url");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");*/

        //soso.addSink(new SinkToGreenplumMultiTransaction(properties));
        soso.addSink(new SinkToGreenplum(properties));

        try {
            env.execute("DataStreamKafkaSinkToGPSample");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
