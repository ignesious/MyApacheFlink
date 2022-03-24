package com.pluralsight.flink;

public class Tweet {

    private String language;
    private String text;

    public Tweet(String language, String text) {
        this.language = language;
        this.text = text;
    }

    public String getLanguage() {
        return language;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return "Tweet{" +
            "language='" + language + '\'' +
            ", text='" + text + '\'' +
            '}';
    }
}
