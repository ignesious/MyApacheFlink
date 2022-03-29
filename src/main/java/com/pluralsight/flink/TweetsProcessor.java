package com.pluralsight.flink;

import java.util.Date;
import java.util.Properties;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import scala.Tuple3;

public class TweetsProcessor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "6wUphMRple2aNV5g78RHqP8AJ");
        props.setProperty(TwitterSource.CONSUMER_SECRET,
            "4hoeuiTNCOxFAxhYloeI0hX4h5cG9UKmItjO5F1hPWJ20i0Mk1");
        props
            .setProperty(TwitterSource.TOKEN, "126874496-kGRx9NlV4Kan2zA699SP2E1EIP40VzWINnEqDuOv");
        props.setProperty(TwitterSource.TOKEN_SECRET,
            "kd0JInKe9x4nIlhM1Tf8wDypIYi3gYuKJ9CK7skJwJTTm");
        ObjectMapper objectMapper = new ObjectMapper();

        // Below is to finthe count of language in a tweet
/*        env.addSource(new TwitterSource(props))
            .map(tweetString -> {
                JsonNode entiretweet = objectMapper.readTree(tweetString);
                String text =
                    entiretweet.get("text") != null ? entiretweet.get("text").asText() : "Mohammed";
                String language =
                    entiretweet.get("lang") != null ? entiretweet.get("lang").asText() : "tamil";
                Tweet tweet = new Tweet(language, text);
                return tweet;
            })
            .keyBy(new KeySelector<Tweet, String>() {
                @Override
                public String getKey(Tweet tweet) throws Exception {
                    return tweet.getLanguage();
                }
            })
            .timeWindow(Time.seconds(10L))
            .apply(new WindowFunction<Tweet, Tuple3<String, Long, Date>, String, TimeWindow>() {
                @Override
                public void apply(String language, TimeWindow timeWindow, Iterable<Tweet> iterable,
                    Collector<Tuple3<String, Long, Date>> collector) throws Exception {

                    long count = 0L;
                    for (Tweet tweets : iterable) {
                        count++;
                    }
                    collector.collect(new Tuple3<>(language, count, new Date(timeWindow.getEnd())));
                }
            })
            .print();*/

        // Below is to find the top language in a window
        DataStream<Tuple3<String, Long, Date>> input = env.addSource(new TwitterSource(props))
            .map(tweetString -> {
                JsonNode entiretweet = objectMapper.readTree(tweetString);
                String text =
                    entiretweet.get("text") != null ? entiretweet.get("text").asText() : "Mohammed";
                String language =
                    entiretweet.get("lang") != null ? entiretweet.get("lang").asText() : "tamil";
                Tweet tweet = new Tweet(language, text);
                return tweet;
            })
            .keyBy(new KeySelector<Tweet, String>() {
                @Override
                public String getKey(Tweet tweet) throws Exception {
                    return tweet.getLanguage();
                }
            })
            .timeWindow(Time.seconds(10L))
            .apply(new WindowFunction<Tweet, Tuple3<String, Long, Date>, String, TimeWindow>() {
                @Override
                public void apply(String language, TimeWindow timeWindow, Iterable<Tweet> iterable,
                    Collector<Tuple3<String, Long, Date>> collector) throws Exception {

                    long count = 0L;
                    for (Tweet tweets : iterable) {
                        count++;
                    }
                    collector.collect(new Tuple3<>(language, count, new Date(timeWindow.getEnd())));
                }
            })
            .timeWindowAll(Time.seconds(10L))
            .apply(
                new AllWindowFunction<Tuple3<String, Long, Date>, Tuple3<String, Long, Date>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow,
                        Iterable<Tuple3<String, Long, Date>> iterable,
                        Collector<Tuple3<String, Long, Date>> collector) throws Exception {

                        String topSpokenlanguage = null;
                        Long count = 0L;
                        Date windowDate = null;

                        for (Tuple3<String, Long, Date> individualLanguage : iterable) {

                            if (individualLanguage._2() > count) {
                                topSpokenlanguage = individualLanguage._1();
                                count = individualLanguage._2();
                                windowDate = individualLanguage._3();
                            }
                        }
                        collector.collect(new Tuple3<>(topSpokenlanguage, count, windowDate));
                    }
                });

        input.print();

        env.execute();

    }
}
