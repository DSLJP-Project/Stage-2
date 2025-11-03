package es.ulpgc.searchengine.indexing.repository;

import java.sql.*;
import java.util.*;

public class DatamartSQLite {
    private final String dbPath;

    public DatamartSQLite(String dbPath) {
        this.dbPath = dbPath;
    }

    private Connection connect() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath + "?busy_timeout=5000");
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL;");
            statement.execute("PRAGMA synchronous=NORMAL;");
        }
        return connection;
    }

    public void initSchema() {
        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            st.executeUpdate("""
                CREATE TABLE IF NOT EXISTS books (
                  id INTEGER PRIMARY KEY,
                  title TEXT,
                  author TEXT,
                  language TEXT,
                  year INTEGER,
                  content TEXT
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
            System.err.println("Database schema initialization error: " + e.getMessage());
        }
    }

    public void deleteIndexForBook(int bookId) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM index_table WHERE book_id = ?")) {

            ps.setInt(1, bookId);
            ps.executeUpdate();

        } catch (SQLException e) {
            System.err.println("Error deleting index for book " + bookId + ": " + e.getMessage());
        }
    }

    public void insertOrUpdateBook(int id, String title, String author, String language, int year, String content) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement("""
                INSERT OR REPLACE INTO books (id, title, author, language, year, content)
                VALUES (?, ?, ?, ?, ?, ?);
            """)) {

            ps.setInt(1, id);
            ps.setString(2, title);
            ps.setString(3, author);
            ps.setString(4, language);
            ps.setInt(5, year);
            ps.setString(6, content);
            ps.executeUpdate();

        } catch (SQLException e) {
            System.err.println("Error inserting or updating book " + id + ": " + e.getMessage());
        }
    }

    public ResultSet getBookById(int id) throws SQLException {
        Connection conn = connect();
        PreparedStatement ps = conn.prepareStatement("SELECT * FROM books WHERE id = ?");
        ps.setInt(1, id);
        return ps.executeQuery();
    }

    public List<Integer> getAllBookIds() throws SQLException {
        List<Integer> ids = new ArrayList<>();
        try (Connection conn = connect();
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT id FROM books")) {

            while (rs.next()) {
                ids.add(rs.getInt("id"));
            }

        } catch (SQLException e) {
            System.err.println("Error fetching book IDs: " + e.getMessage());
        }
        return ids;
    }

    public void insertIndex(int bookId, Map<String, List<Integer>> index) {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement("""
                INSERT INTO index_table (term, book_id, positions)
                VALUES (?, ?, ?);
            """)) {

            for (var entry : index.entrySet()) {
                ps.setString(1, entry.getKey());
                ps.setInt(2, bookId);
                ps.setString(3, entry.getValue().toString());
                ps.addBatch();
            }
            ps.executeBatch();

        } catch (SQLException e) {
            System.err.println("Error inserting index for book " + bookId + ": " + e.getMessage());
        }
    }

    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();

        try (Connection conn = connect(); Statement st = conn.createStatement()) {
            ResultSet rs1 = st.executeQuery("SELECT COUNT(*) AS books FROM books");
            if (rs1.next()) stats.put("books_indexed", rs1.getInt("books"));

            ResultSet rs2 = st.executeQuery("SELECT COUNT(*) AS terms FROM index_table");
            if (rs2.next()) stats.put("terms_indexed", rs2.getInt("terms"));

            stats.put("last_update", new java.util.Date().toString());
        } catch (SQLException e) {
            System.err.println("Error fetching stats: " + e.getMessage());
            stats.put("error", e.getMessage());
        }

        return stats;
    }
}
