package com.sinobridge.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.HashMap;
import java.util.Properties;

/**
 * kafka消息： {event_id: '1001',event_code: 2008, xuid: 'abcd1002', client_id: 'abcdefg'}
 * 根据event_id查询map中对应的event_name，将查询到event_name增加到消息体，并打印输出
 */
public class DataStreamKafkaSample01 {
    public static void main(String[] args) {
        //1、获得一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2、加载/创建 初始化数据
        String topic = "event";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.250.11:9092");
//        prop.setProperty("zookeeper.connect","192.25.105.188:2181");
        prop.setProperty("group.id","test01");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);

        kafkaConsumer.setStartFromEarliest();
        DataStreamSource<String> text = env.addSource(kafkaConsumer);

        //3、指定操作数据的transaction算子
        HashMap<String, String> map = new HashMap<>();
        map.put("1001", "1001name");
        SingleOutputStreamOperator<String> fetchEventName = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String message) throws Exception {
                JSONObject jsonObject = JSON.parseObject(message);
                String event_id = (String) jsonObject.get("event_id");
                jsonObject.put("event_name", map.get(event_id));
                return JSON.toJSONString(jsonObject);
            }
        });

        //4、指定数据的目的地
        fetchEventName.print().setParallelism(1);

        //5、调用execute()方法来触发执行程序
        try {
            env.execute("DataStreamKafkaSample01");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
