package com.pluralsight.flink;

import java.util.Arrays;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class FilterMoviesBatch {

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long, String, String>> lines = env.readCsvFile(
            "/Users/mmohideen/Desktop/vmware/flinkcodebase/flink-pluralsight-course/datafiles/ml-latest-small/movies.csv")
            .ignoreFirstLine()
            .ignoreInvalidLines()
            .parseQuotedStrings('"')
            .types(Long.class, String.class, String.class);

        DataSet<Movie> movieDataSet = lines
            .map(new MapFunction<Tuple3<Long, String, String>, Movie>() {

                @Override
                public Movie map(Tuple3<Long, String, String> readTuple) throws Exception {
                    String genres[] = readTuple.f2.split("\\|");
                    Movie movie = new Movie(readTuple.f0, readTuple.f1, Arrays.asList(genres));
                    return movie;
                }
            }).filter(movie -> {
                if (movie.getGenres().contains("Romance")) {
                    return true;
                } else {
                    return false;
                }
            });

        try {
            movieDataSet.print();

        } catch (Exception exception) {
            exception.printStackTrace();
        }



    }
}


