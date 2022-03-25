package com.pluralsight.flink;

import java.util.Properties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

public class TweetsProcessor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "6wUphMRple2aNV5g78RHqP8AJ");
        props.setProperty(TwitterSource.CONSUMER_SECRET,
            "4hoeuiTNCOxFAxhYloeI0hX4h5cG9UKmItjO5F1hPWJ20i0Mk1");
        props
            .setProperty(TwitterSource.TOKEN, "126874496-kGRx9NlV4Kan2zA699SP2E1EIP40VzWINnEqDuOv");
        props.setProperty(TwitterSource.TOKEN_SECRET,
            "kd0JInKe9x4nIlhM1Tf8wDypIYi3gYuKJ9CK7skJwJTTm");
        ObjectMapper objectMapper = new ObjectMapper();
        env.addSource(new TwitterSource(props))
            .map(tweetString -> {
                JsonNode entiretweet = objectMapper.readTree(tweetString);
                String text =
                    entiretweet.get("text") != null ? entiretweet.get("text").asText() : "Mohammed";
                String language =
                    entiretweet.get("lang") != null ? entiretweet.get("lang").asText() : "tamil";
                Tweet tweet = new Tweet(language, text);
                return tweet;
            })
            .print();

        env.execute();

    }
}
