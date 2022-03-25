package com.pluralsight.flink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class MovieJoin {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long, String, String>> movies = env.readCsvFile(
            "/Users/mmohideen/Desktop/vmware/flinkcodebase/flink-pluralsight-course/datafiles/ml-latest-small/movies.csv")
            .ignoreFirstLine()
            .ignoreInvalidLines()
            .parseQuotedStrings('"')
            .types(Long.class, String.class, String.class);

        DataSet<Tuple3<Long, Long, Double>> rating = env.readCsvFile(
            "/Users/mmohideen/Desktop/vmware/flinkcodebase/flink-pluralsight-course/datafiles/ml-latest-small/ratings.csv")
            .ignoreFirstLine()
            .ignoreInvalidLines()
            .includeFields(true, true, true, false)
            .types(Long.class, Long.class, Double.class);

        movies.join(rating)
            .where(0).equalTo(1)
            .with(
                new JoinFunction<Tuple3<Long, String, String>, Tuple3<Long, Long, Double>, Tuple4<String, String, Double, Long>>() {
                    @Override
                    public Tuple4<String, String, Double, Long> join(
                        Tuple3<Long, String, String> movieTuple,
                        Tuple3<Long, Long, Double> ratingTuple) throws Exception {
                        String name = movieTuple.f1;
                        String genre = movieTuple.f2.split("\\|")[0];
                        Double rating = ratingTuple.f2;
                        Long userId = ratingTuple.f0;
                        return new Tuple4<>(name, genre, rating, userId);
                    }
                })
            .groupBy(0)
            .reduceGroup(
                new GroupReduceFunction<Tuple4<String, String, Double, Long>, Tuple2<String, Double>>() {

                    @Override
                    public void reduce(Iterable<Tuple4<String, String, Double, Long>> iterable,
                        Collector<Tuple2<String, Double>> collector) throws Exception {

                        String movieName = null;
                        Double totalRating = 0.0;
                        int count = 0;

                        for (Tuple4<String, String, Double, Long> joinTuple : iterable) {
                            movieName = joinTuple.f0;
                            totalRating += joinTuple.f2;
                            count++;
                        }

                        collector.collect(new Tuple2<>(movieName, totalRating / count));

                    }
                })
            .print();


    }
}
