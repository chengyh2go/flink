package com.sinobridge.flink.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 输入数据格式：jack,client-id-jack
 * 需求：根据client-id-jack从http接口获取xuid，转换成：（jack，client-id-jack，y2l920wu7l)
 */
public class HttpApiJoinDemo01 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("192.168.250.20", 9001);

        SingleOutputStreamOperator<Tuple2<String, String>> mapStream = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] strings = s.split(",");
                return new Tuple2<>(strings[0], strings[1]);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, String>> customMapResult = mapStream.map(new CustomMapJoin());

        customMapResult.print();

        try {
            env.execute("HttpApiJoinDemo01");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    static class CustomMapJoin extends RichMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {

        @Override
        public Tuple3<String, String, String> map(Tuple2<String, String> tup2) throws Exception {
            String xuid = retrieveXUIDFromApi(tup2.f1);
            return new Tuple3<>(tup2.f0,tup2.f1,xuid);
        }

        private String retrieveXUIDFromApi(String clientID) {
            return HttpUtils.sendGet("http://192.168.250.1:8080/xuid","clientid="+clientID);
        }
    }
}


