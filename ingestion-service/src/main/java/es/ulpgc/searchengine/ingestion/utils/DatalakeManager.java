package es.ulpgc.searchengine.ingestion.utils;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Stream;

public class DatalakeManager {
    private static final String BASE = "./datalake";

    public static Path save(int bookId, String rawText) {
        try {
            LocalDateTime now = LocalDateTime.now();
            Path bookDir = Paths.get(
                    BASE,
                    now.toLocalDate().toString().replace("-", ""),
                    String.format("%02d", now.getHour()),
                    String.format("%02d", now.getMinute()),
                    String.valueOf(bookId)
            );

            Files.createDirectories(bookDir);

            List<String> lines = Arrays.asList(rawText.split("\n"));
            int start = findStart(lines);
            int end = findEnd(lines);

            List<String> header = extractHeader(lines, start);
            List<String> body = extractBody(lines, start, end);

            Files.write(bookDir.resolve("header.txt"), header);
            Files.write(bookDir.resolve("body.txt"), body);
            Files.writeString(bookDir.resolve("book.txt"), rawText);

            System.out.printf("Book %d saved at: %s%n", bookId, bookDir);
            System.out.printf("- Header: %d lines%n- Body: %d lines%n", header.size(), body.size());
            if (!header.isEmpty())
                System.out.println("- First header line: " + header.get(0));

            return bookDir;

        } catch (IOException e) {
            logError("Error saving book " + bookId, e);
            return null;
        }
    }
    public static boolean exists(int bookId) {
        try (Stream<Path> paths = Files.walk(Paths.get(BASE))) {
            return paths.anyMatch(p -> p.getFileName().toString().equals(String.valueOf(bookId)));
        } catch (IOException e) {
            logError("Error checking if book exists: " + bookId, e);
            return false;
        }
    }
    public static Map<String, Object> list() {
        List<Map<String, Object>> books = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(Paths.get(BASE))) {
            paths.filter(Files::isDirectory)
                    .filter(p -> p.getFileName().toString().matches("\\d+"))
                    .forEach(bookPath -> safeAddBookInfo(books, bookPath));
        } catch (IOException e) {
            logError("Error listing datalake books", e);
        }

        return Map.of("count", books.size(), "books", books);
    }

    public static List<Path> findBooksByTimestamp(String date, String hour, String minute) {
        List<Path> results = new ArrayList<>();
        try {
            Path targetDir = Paths.get(BASE, date, hour, minute);
            if (Files.exists(targetDir)) {
                try (Stream<Path> stream = Files.list(targetDir)) {
                    stream.filter(Files::isDirectory).forEach(results::add);
                }
            }
        } catch (IOException e) {
            logError("Error searching by timestamp", e);
        }
        return results;
    }

    private static int findStart(List<String> lines) {
        for (int i = 0; i < Math.min(100, lines.size()); i++) {
            String line = lines.get(i).toLowerCase();
            if (line.contains("*** start of") || line.contains("***start of")) {
                return i + 1;
            }
        }
        return -1;
    }

    private static int findEnd(List<String> lines) {
        for (int i = 0; i < Math.min(100, lines.size()); i++) {
            String line = lines.get(i).toLowerCase();
            if (line.contains("*** end of") || line.contains("***end of")) {
                return i;
            }
        }
        return lines.size();
    }

    private static List<String> extractHeader(List<String> lines, int start) {
        if (start == -1) {
            int limit = Math.min(50, lines.size());
            return lines.subList(0, limit);
        } else {
            return lines.subList(0, Math.max(0, start - 1));
        }
    }

    private static List<String> extractBody(List<String> lines, int start, int end) {
        if (start >= 0 && end > start)
            return lines.subList(start, end);
        else
            return lines.subList(Math.min(50, lines.size()), lines.size());
    }

    private static void safeAddBookInfo(List<Map<String, Object>> books, Path bookPath) {
        try {
            int bookId = Integer.parseInt(bookPath.getFileName().toString());
            Path minute = bookPath.getParent();
            Path hour = minute != null ? minute.getParent() : null;
            Path date = hour != null ? hour.getParent() : null;
            String timestamp = (date != null && hour != null && minute != null)
                    ? date.getFileName() + "/" + hour.getFileName() + "/" + minute.getFileName()
                    : "unknown";

            books.add(Map.of(
                    "id", bookId,
                    "timestamp", timestamp,
                    "path", bookPath.toString()
            ));
        } catch (Exception e) {
            logError("Error parsing book directory: " + bookPath, e);
        }
    }

    private static void logError(String message, Exception e) {
        System.err.println(message + ": " + e.getMessage());
    }
}
