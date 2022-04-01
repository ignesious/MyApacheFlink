package com.pluralsight.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class StreamingGameScores {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.socketTextStream("localhost", 9000);

        dataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String nameOfPlayer = null;
                Integer score = 0;
                String input[] = s.split(",");
                nameOfPlayer = input[0];
                score = Integer.parseInt(input[1]);
                return new Tuple2<>(nameOfPlayer, score);
            }
        })
            .keyBy(input -> input.f0)
            .window(GlobalWindows.create())
            .trigger(PurgingTrigger.of(CountTrigger.of(3)))
            .apply(
                new WindowFunction<Tuple2<String, Integer>, Tuple1<Long>, String, GlobalWindow>() {
                    @Override
                    public void apply(String s, GlobalWindow globalWindow,
                        Iterable<Tuple2<String, Integer>> iterable,
                        Collector<Tuple1<Long>> collector)
                        throws Exception {
                        long finalResult = 0L;
                        long count = 0L;
                        long sumScores = 0L;

                        for (Tuple2<String, Integer> results : iterable) {
                            sumScores += results.f1;
                            count++;
                        }
                        finalResult = sumScores / count;
                        collector.collect(new Tuple1<>(finalResult));
                    }
                })
            .print();

        env.execute("Streming Game scores");

    }
}
