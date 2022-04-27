package com.shawood.ch05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Int;

import java.util.ArrayList;
import java.util.Properties;

/**
 * 该类用于: TODO
 *
 * @author shawood
 * @version 1.0
 * @date 2022-04-27
 */
public class SourceTest {
    public static void main(String[] args) throws Exception{
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取数据源
        // 1.从文件读取
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2.从集合中读取
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);


        ArrayList<Event> events= new ArrayList<>();
        events.add(new Event("Shadow", "./home", 1000L));
        events.add(new Event("Shawood", "./cart", 2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3.从元素读取数据
        DataStreamSource<Event> elementSource = env.fromElements(
                new Event("Shadow", "./home", 1000L),
                new Event("Shawood", "./cart", 2000L)
        );

        // 4.从socket文本流读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("bd2", 7777);

        // 5.从kafka中读取流式数据
        // kafka设置
        System.out.println("开始连接kafka");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","47.100.98.246:9092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks",
                new SimpleStringSchema(), properties));
        System.out.println("kafka连接成功");

        // datasink
//        stream1.print();
//        numStream.print();
//        stream2.print();
//        elementSource.print();
//        socketTextStream.print("4");
        kafkaStream.print();
        // 运行
        env.execute();
    }
}
