package com.shawood.ch05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
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
public class TransformFilterTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Shadow", "./home", 1000L),
                new Event("Shawood", "./cart", 2000L)
        );

        // 1.传入一个实现了FilterFunction的类的对象
        SingleOutputStreamOperator<Event> filter1 = streamSource.filter(new MyFilter());

        // 2.传入一个匿名类实现FilterFunction接口
        SingleOutputStreamOperator<Event> filter2 = streamSource.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return event.user.equals("Shawood");
            }
        });

        // 3.传入lambda表达式
        SingleOutputStreamOperator<Event> filter3 = streamSource.filter(data -> data.user.equals("Shadow"));

        filter3.print("lambda : Shadow click ");

        env.execute();


    }

     /**
      * 实现一个自定义的FilterFunction
      */
    private static class MyFilter implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            return event.user.equals("Shadow");
        }
    }
}
