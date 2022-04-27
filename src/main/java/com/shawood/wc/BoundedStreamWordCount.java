package com.shawood.wc;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception{
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/words.txt");

        // 3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple =
                lineDataStreamSource.flatMap((String line,Collector<Tuple2<String , Long>> out) ->{
            String[] words = line.split(" ");
            for (String word: words){
                out.collect(Tuple2.of(word,1L));
            }
        })
                        .returns(new TypeHint<Tuple2<String, Long>>(){
                            @Override
                            public TypeInformation<Tuple2<String, Long>> getTypeInfo() {
                                return super.getTypeInfo();
                            }
                        });
//                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组统计
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String,Long>> sum = wordAndOneKeyedStream.sum(1);

        // 6.打印
        sum.print();

        // 7. 启动执行
        env.execute();
    }
}