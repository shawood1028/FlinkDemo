package com.shawood.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author shawood
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("47.100.98.246",8081,
                "target/FlinkDemo-1.0-SNAPSHOT.jar");

        // 从参数中提取主机名和端口号
//        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        String hostname = parameterTool.get("host");
//        Integer port = parameterTool.getInt("port");

        // 2. 读取文本流
        DataStreamSource<String> lineDataStream = env.socketTextStream("bd1",7777);

        // 3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple =
                lineDataStream.flatMap((String line, Collector<Tuple2<String , Long>> out) ->{
                            String[] words = line.split(" ");
                            for (String word: words){
                                out.collect(Tuple2.of(word,1L));
                            }
                        })
                        .returns(Types.TUPLE(Types.STRING, Types.LONG));

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
