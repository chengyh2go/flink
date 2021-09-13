package com.sinobridge.flink.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;

public class WaterMarkSample {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置使用数据产生的时间：eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置全局并行度为1
        env.setParallelism(1);

        //设置自动周期性的产生watermark（不设置也是默认200ms）
        env.getConfig().setAutoWatermarkInterval(200);

        DataStreamSource<String> text = env.socketTextStream("192.168.250.20", 9001);

        //将数据转换成tuple2的格式
        SingleOutputStreamOperator<Tuple2<String, Long>> map = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0], Long.parseLong(split[1]));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> waterMarkStream = map.assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

                    Long currentMaxTimestamp = 0L;
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    //从数据流中抽取timestamp作为eventTime
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> tup, long recordTimestamp) {
                        Long timestamp = tup.f1;
                        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
                        Watermark currentWatermark = getCurrentWatermark();
                        assert currentWatermark != null;
                        System.out.println("key: " + tup.f0 +
                                " eventTime: [" + tup.f1 + "| " +
                                sdf.format(tup.f1) + "], currentMaxTimeStamp: [" + currentMaxTimestamp +"| "
                                + sdf.format(currentMaxTimestamp) + "], currentWatermark: [" +
                                currentWatermark.getTimestamp() + "| " + sdf.format(currentWatermark.getTimestamp()) + "]"
                        ) ;


                        return timestamp;
                    }

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        //计算当前的watermark，注意没有别的作用，就是为了打印出来方便观察数据
                        //watermark = currentMaxTimestamp - 最大允许乱序时间(OutOfOrderness)，这里设置的是10秒
                        return new Watermark(currentMaxTimestamp - 10000L);
                    }
                });

        //waterMarkStream做keyBy操作
        //这里可以调window，用原生的方式，但是就不要用processing time了，要用event time
        waterMarkStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> tup) throws Exception {
                return tup.f0;
            }
        })
                //按照消息的event time分配窗口，和调用TimeWindow效果一样
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                //这里使用全量聚合的方式，而不使用增量聚合，所以要使用apply或者process
                //这里就是使用全量聚合的方式处理window中的数据
                //这里面就要接收一个windowFunction,有4个输入参数：
                //参数1： String key，这是指定key的类型
                //参数2：TimeWindow，这个例子里使用的是基于time的window
                //参数3：Tuple2[String, Long]，这是数据类型，这里是tuple2
                //参数4：Collector<String> out，collect的输出类型是string

                .apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {

                        //将window中的数据保存到List中
                        ArrayList<Long> list = new ArrayList<>();

                        //参数里的input，就是窗口里的所有数据
                        //注意，只有窗口触发的时候，这个内层的apply函数才会执行
                        //窗口触发之后，就会把input里的数据全都传过来
                        //那么就可以迭代窗口里的数据

                        //将tup里的第2列，即时间，添加到array中
                        for (Tuple2<String, Long> tup: input) {
                            list.add(tup.f1);
                        }

                        //排序
                        Collections.sort(list);

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        String result = key + "," + list.size() + ","
                                + sdf.format(list.get(0)) + sdf.format(list.get(list.size() -1))
                                + sdf.format(window.getStart()) + sdf.format(window.getEnd())
                                ;

                        out.collect(result);

                    }
                }).print();


        try {
            env.execute("WaterMarkSample");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //调试输入数据

        /*
        0001,1790820682000
        输出：key: 0001 eventTime: [1790820682000| 2026-10-01 10:11:22],
        currentMaxTimeStamp: [1790820682000| 2026-10-01 10:11:22],
        currentWatermark: [1790820672000| 2026-10-01 10:11:12]

        */

    }
}
