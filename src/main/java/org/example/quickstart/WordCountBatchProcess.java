package org.example.quickstart;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class WordCountBatchProcess
{
    public static void main( String[] args ) throws Exception {
        System.out.println( "Hello World!" );
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stringDataSource = environment.readTextFile("intput/words.txt");
        FlatMapOperator<String, Tuple2<String, Long>> flatMapOperator = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                Arrays.stream(line.split(" ")).forEach(
                        word -> collector.collect(Tuple2.of(word, 1l))
                );
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> unsortedGrouping = flatMapOperator.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = unsortedGrouping.sum(1);

        stringDataSource.print("string data source");
        flatMapOperator.print("flat map : ");
        sum.print();

    }

}
