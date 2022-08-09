package com.atguigu.chapter11;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import com.atguigu.chapter11.source.AutodialerNumberDetail;
import com.atguigu.chapter11.source.AutodialerNumberSource;
import com.atguigu.chapter11.source.SmsDetail;
import com.atguigu.chapter11.source.SmsSource;
import javafx.scene.control.Tab;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

public class CumulateWindowJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SmsDetail> smsStream = env.addSource(new SmsSource());
        SingleOutputStreamOperator<AutodialerNumberDetail> dialerStream = env.addSource(new AutodialerNumberSource());

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表，并指定时间属性
        Table smsTable = tableEnv.fromDataStream(
                smsStream,
                $("taskId"),
                $("smsId"),
                $("ts").proctime()
        );

        Table dialerTable = tableEnv.fromDataStream(
                dialerStream,
                $("taskId"),
                $("numberId"),
                $("ts").proctime()
        );

        // 为方便在SQL中引用，在环境中注册表EventTable
        tableEnv.createTemporaryView("SmsTable", smsTable);
        tableEnv.createTemporaryView("dialerTable", dialerTable);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("+08:00"));
        // 设置累积窗口，执行SQL统计查询
        Table smsResult = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "taskId, " +
                                "window_end AS endT, " +
                                "COUNT(distinct smsId) AS cnt " +
                                "FROM TABLE( " +
                                "CUMULATE( TABLE SmsTable, " +    // 定义累积窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '5' SECOND, " +
                                "INTERVAL '1' HOUR)) " +
                                "GROUP BY taskId, window_start, window_end "
                );
        Table dialerResult = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "taskId, " +
                                "window_end AS endT, " +
                                "COUNT(distinct numberId) AS cnt " +
                                "FROM TABLE( " +
                                "CUMULATE( TABLE dialerTable, " +    // 定义累积窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '5' SECOND, " +
                                "INTERVAL '1' HOUR)) " +
                                "GROUP BY taskId, window_start, window_end "
                );


        DataStream<Row> smsRowStream = tableEnv.toDataStream(smsResult);
        DataStream<Row> dialerRowStream = tableEnv.toDataStream(dialerResult);
        smsRowStream.coGroup(dialerRowStream)
                .where(x -> x.getField("taskId"))
                .equalTo(y -> y.getField("taskId"))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .apply(new RichCoGroupFunction<Row, Row, String>() {
                    @Override
                    public void coGroup(Iterable<Row> first, Iterable<Row> second, Collector<String> collector) throws Exception {
                        if(!first.iterator().hasNext() && !second.iterator().hasNext()){
                            return;
                        }

                        if(!second.iterator().hasNext()){
                            for(Row r1 : first) {
                                collector.collect(String.format("taskId:%s, time:%s, smsCount:%s, dialerCount:%s"
                                        , r1.getField("taskId")
                                        , r1.getField("endT")
                                        , r1.getField("cnt")
                                        , "0"
                                ));
                            }
                        }

                        if(!first.iterator().hasNext()){
                            for(Row r2 : second) {
                                collector.collect(String.format("taskId:%s, time:%s, smsCount:%s, dialerCount:%s"
                                        , r2.getField("taskId")
                                        , r2.getField("endT")
                                        , "0"
                                        , r2.getField("cnt")
                                ));
                            }

                        }

                        for(Row r1 : first){
                            for(Row r2: second){
                                if(Objects.equals(r1.getField("endT"), r2.getField("endT"))){
                                    collector.collect(String.format("taskId:%s, time:%s, smsCount:%s, dialerCount:%s"
                                            , r1.getField("taskId")
                                            , r1.getField("endT")
                                            , r1.getField("cnt")
                                            , r2.getField("cnt")
                                    ));
                                }
                            }
                        }
                    }
                }).addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        SinkFunction.super.invoke(value, context);
                        System.out.println(LocalDateTime.now());
                        System.out.println(value);
                        System.out.println("----------------");
                    }
                });



        env.execute();
    }
}

