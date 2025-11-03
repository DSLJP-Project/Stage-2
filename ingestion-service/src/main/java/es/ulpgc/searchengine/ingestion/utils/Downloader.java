package es.ulpgc.searchengine.ingestion.utils;

import java.net.http.*;
import java.net.URI;
import java.time.Duration;

public class Downloader {
    private static final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .build();

    public static String get(String url) throws Exception {
        System.out.println("Downloading from: " + url);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        System.out.println("Response code: " + res.statusCode());
        if (res.statusCode() >= 200 && res.statusCode() < 300) {
            return res.body();
        } else {
            throw new RuntimeException("HTTP " + res.statusCode() + " when fetching " + url);
        }
    }
}

