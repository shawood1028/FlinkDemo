package com.shawood.ch05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 该类用于: TODO
 *
 * @author shawood
 * @version 1.0
 * @date 2022-04-28
 */
public class TransformMapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 进行转换计算
        // 1.使用自定义类,实现MapFunction接口
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Shadow", "./home", 1000L),
                new Event("Shawood", "./cart", 2000L)
        );

        // 2.使用匿名类,实现MapFunction接口
        SingleOutputStreamOperator<String> map2 = streamSource.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });

        // 传入lambda表达式
        SingleOutputStreamOperator<String> map3 = streamSource.map(data -> data.user);

        SingleOutputStreamOperator<String> map = streamSource.map(new MyMapper());

        map3.print();

        env.execute();
    }

    public static class MyMapper implements MapFunction<Event,String> {
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
