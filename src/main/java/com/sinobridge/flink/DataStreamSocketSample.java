package com.sinobridge.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 需求：通过socket实时产生一些单词
 * 使用flink实时接收数据
 * 对指定时间窗口（例如：2秒）的数据进行聚合统计
 * 并且把时间窗口内的结果打印到控制台
 */

public class DataStreamSocketSample {
    public static void main(String[] args) {
        //1、获得一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2、加载/创建 初始化数据
        //连接socket获取输入数据
        DataStreamSource<String> text = env.socketTextStream("192.168.250.20", 9001);

        //3、指定操作数据的transaction算子
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount =
                text.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> out) throws Exception {
                        String[] words = line.split(" ");
                        for (String word: words) {
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return new Tuple2<>(s,1);
                    }
                })
                .keyBy((KeySelector<Tuple2<String, Integer>, Object>) tup -> tup.f0).sum(1);

        //4、指定数据的目的地
        wordCount.print().setParallelism(1);

        //5、调研execute()方法来触发执行程序
        //flink程序是延迟计算的，只有真正调用execute()方法的时候才会真正触发执行程序
        try {
            env.execute("DataStreamSocketSample");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
