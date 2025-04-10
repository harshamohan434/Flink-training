package org.example.quickstart;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountStreamProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.readTextFile("intput/words.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> wordCount = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                Arrays.stream(line.split(" ")).forEach(
                        word -> collector.collect(Tuple2.of(word, 1l))
                );
            }
        });

        wordCount.print("word count");

        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordCount.keyBy(tuple -> tuple.f0);
        keyedStream.print("Keyed stream");

        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);
        sum.print("sum");

        env.execute();

    }
}
