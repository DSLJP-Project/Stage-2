package es.ulpgc.searchengine.search.repository;

import java.sql.*;
import java.util.*;

public class DatamartSQLite {
    private final String dbPath;

    public DatamartSQLite(String dbPath) {
        this.dbPath = dbPath;
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + dbPath);
    }

    public void initSchema() {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.executeUpdate("""
                CREATE TABLE IF NOT EXISTS books (
                  id INTEGER PRIMARY KEY,
                  title TEXT,
                  author TEXT,
                  language TEXT,
                  year INTEGER
                );
            """);
            st.executeUpdate("""
                CREATE TABLE IF NOT EXISTS index_table (
                  term TEXT,
                  book_id INTEGER,
                  positions TEXT
                );
            """);
        } catch (SQLException e) {
            System.err.println("DB init error: " + e.getMessage());
        }
    }

    public List<Map<String, Object>> query(String term, String author, String language, Integer year) {
        List<Map<String, Object>> results = new ArrayList<>();
        StringBuilder sql = new StringBuilder("""
            SELECT DISTINCT b.id, b.title, b.author, b.language, b.year
            FROM books b
            JOIN index_table i ON b.id = i.book_id
            WHERE i.term = ?
        """);
        if (author != null && !author.isBlank()) sql.append(" AND b.author LIKE ?");
        if (language != null && !language.isBlank()) sql.append(" AND b.language LIKE ?");
        if (year != null) sql.append(" AND b.year = ?");

        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int paramIndex = 1;
            ps.setString(paramIndex++, term.toLowerCase());
            if (author != null && !author.isBlank()) ps.setString(paramIndex++, "%" + author + "%");
            if (language != null && !language.isBlank()) ps.setString(paramIndex++, "%" + language + "%");
            if (year != null) ps.setInt(paramIndex++, year);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("book_id", rs.getInt("id"));
                row.put("title", rs.getString("title"));
                row.put("author", rs.getString("author"));
                row.put("language", rs.getString("language"));
                row.put("year", rs.getInt("year"));
                results.add(row);
            }
        } catch (SQLException e) {
            System.err.println("Query error: " + e.getMessage());
        }

        return results;
    }

    public List<Map<String, Object>> queryByYearRange(Integer startYear, Integer endYear) {
        List<Map<String, Object>> results = new ArrayList<>();

        String sql = """
        SELECT DISTINCT b.id, b.title, b.author, b.language, b.year
        FROM books b
        WHERE b.year BETWEEN ? AND ?
        ORDER BY b.year
    """;

        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, startYear);
            ps.setInt(2, endYear);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("book_id", rs.getInt("id"));
                row.put("title", rs.getString("title"));
                row.put("author", rs.getString("author"));
                row.put("language", rs.getString("language"));
                row.put("year", rs.getInt("year"));
                results.add(row);
            }
        } catch (SQLException e) {
            System.err.println("Year range query error: " + e.getMessage());
        }

        return results;
    }

    public Map<String, Object> getSearchStats() {
        Map<String, Object> stats = new HashMap<>();
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            ResultSet rs1 = st.executeQuery("SELECT COUNT(*) AS total FROM books");
            rs1.next();
            stats.put("total_books", rs1.getInt("total"));
            ResultSet rs2 = st.executeQuery("SELECT COUNT(DISTINCT term) AS terms FROM index_table");
            rs2.next();
            stats.put("unique_terms", rs2.getInt("terms"));
            ResultSet rs3 = st.executeQuery("""
            SELECT year, COUNT(*) as count 
            FROM books 
            WHERE year != -1 
            GROUP BY year 
            ORDER BY year
        """);

            Map<Integer, Integer> booksByYear = new HashMap<>();
            while (rs3.next()) {
                booksByYear.put(rs3.getInt("year"), rs3.getInt("count"));
            }
            stats.put("books_by_year", booksByYear);
            ResultSet rs4 = st.executeQuery("""
            SELECT term, COUNT(*) as frequency 
            FROM index_table 
            GROUP BY term 
            ORDER BY frequency DESC 
            LIMIT 20
        """);

            Map<String, Integer> popularTerms = new HashMap<>();
            while (rs4.next()) {
                popularTerms.put(rs4.getString("term"), rs4.getInt("frequency"));
            }
            stats.put("popular_terms", popularTerms);

        } catch (SQLException e) {
            stats.put("error", e.getMessage());
        }
        return stats;
    }
}

