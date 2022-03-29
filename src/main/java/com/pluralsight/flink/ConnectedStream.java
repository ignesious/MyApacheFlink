package com.pluralsight.flink;

import java.util.logging.Logger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectedStream {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> control = env.fromElements("Mohammed", "Mohideen").keyBy(x -> x);
        DataStream<String> streamOfWords = env.fromElements("Apache", "DROP", "Flink", "IGNORE").keyBy(x -> x);

        control
            .connect(streamOfWords)
            .map(new myCoMapFunction())
            .print();

        env.execute();

    }

    private static class myCoMapFunction implements CoMapFunction<String, String, String> {

        @Override
        public String map1(String s) throws Exception {
            System.out.println("Hey Mohammed1 this is -->>   " + s);
            return "ku";
        }

        @Override
        public String map2(String s) throws Exception {
            System.out.println("Hey Mohammed2 this is-->    "+ s);
            return "ku";
        }
    }
}




