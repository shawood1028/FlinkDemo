package com.shawood.ch05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 该类用于: TODO
 *
 * @author shawood
 * @version 1.0
 * @date 2022-04-28
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<Event> customSource = env.addSource(new ClickSource());

        // 实现自定义的并行SourceFunction
        DataStreamSource<Integer> parallelCustomSource = env.addSource(new ParallelCustomSource()).setParallelism(2);

//        customSource.print();

        parallelCustomSource.print();

        env.execute();

    }

    public static class ParallelCustomSource implements ParallelSourceFunction<Integer>{
        private Boolean running = true;
        private Random random = new Random();


        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while (running){
                sourceContext.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
