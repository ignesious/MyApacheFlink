package com.pluralsight.flink;

import java.util.List;
import java.util.Set;

public class Movie {
    private long movieId;
    private String name;
    private List<String> genres;

    public Movie(Long movieId,String name, List<String> genres) {
        this.movieId = movieId;
        this.name = name;
        this.genres = genres;
    }

    public long getMovieId() {
        return movieId;
    }

    public String getName() {
        return name;
    }

    public List<String> getGenres() {
        return genres;
    }

    @Override
    public String toString() {
        return "Movie{" +
            "movieId=" + movieId +
            ", name='" + name + '\'' +
            ", genres=" + genres +
            '}';
    }

}
