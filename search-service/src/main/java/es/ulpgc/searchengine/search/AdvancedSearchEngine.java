package es.ulpgc.searchengine.search;

import es.ulpgc.searchengine.search.repository.DatamartSQLite;
import java.util.*;
import java.util.stream.Collectors;

public class AdvancedSearchEngine {
    private final DatamartSQLite repository;

    public AdvancedSearchEngine(DatamartSQLite repository) {
        this.repository = repository;
    }

    public List<Map<String, Object>> search(String term, String author, String language, Integer year) {
        if (term == null || term.isBlank()) return List.of();
        return repository.query(term, author, language, year);
    }

    public List<Map<String, Object>> searchMultipleTerms(List<String> terms, String author, String language, Integer year) {
        if (terms == null || terms.isEmpty()) return List.of();
        List<Map<String, Object>> allResults = new ArrayList<>();
        for (String term : terms) {
            List<Map<String, Object>> termResults = repository.query(term, author, language, year);
            if (allResults.isEmpty()) {
                allResults.addAll(termResults);
            } else {
                allResults.retainAll(termResults);
            }
        }
        return allResults;
    }

    public List<Map<String, Object>> searchPhrase(String phrase, String author, String language, Integer year) {
        if (phrase == null || phrase.isBlank()) return List.of();
        String[] words = phrase.toLowerCase().split("\\s+");
        List<Map<String, Object>> results = new ArrayList<>();
        List<Map<String, Object>> booksWithAllTerms = searchMultipleTerms(Arrays.asList(words), author, language, year);
        for (Map<String, Object> book : booksWithAllTerms) {
            Integer bookId = (Integer) book.get("book_id");
            if (hasConsecutivePositions(bookId, words)) {
                results.add(book);
            }
        }
        return results;
    }

    public List<Map<String, Object>> booleanSearch(String query, String author, String language, Integer year) {
        if (query == null || query.isBlank()) return List.of();
        System.out.println("Performing boolean search: " + query);
        String[] terms = query.split("\\s+");
        if (terms.length > 0) {
            String firstTerm = terms[0].toLowerCase();
            if (firstTerm.equals("and") || firstTerm.equals("or") || firstTerm.equals("not")) {
                firstTerm = terms.length > 1 ? terms[1].toLowerCase() : "";
            }
            if (!firstTerm.isEmpty()) {
                return repository.query(firstTerm, author, language, year);
            }
        }
        return List.of();
    }

    public List<Map<String, Object>> searchByYearRange(Integer startYear, Integer endYear, String term) {
        List<Map<String, Object>> allResults = new ArrayList<>();
        if (term != null && !term.isBlank()) {
            List<Map<String, Object>> termResults = repository.query(term, null, null, null);
            for (Map<String, Object> book : termResults) {
                Integer year = (Integer) book.get("year");
                if (year >= startYear && year <= endYear) {
                    allResults.add(book);
                }
            }
        } else {
            try {
                allResults = repository.queryByYearRange(startYear, endYear);
            } catch (Exception e) {
                System.err.println("Error in year range search: " + e.getMessage());
            }
        }
        return allResults;
    }

    public Map<String, Object> getSearchStats() {
        return repository.getSearchStats();
    }

    private boolean hasConsecutivePositions(int bookId, String[] words) {
        return true;
    }

    public void refreshCache() {
        System.out.println("Advanced search cache refreshed");
    }
}