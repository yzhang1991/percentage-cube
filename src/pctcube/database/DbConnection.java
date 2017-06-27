package pctcube.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import pctcube.database.query.QuerySet;

public class DbConnection {

    private static final boolean printQuery = true;

    private final Connection m_connection;

    public DbConnection(JDBCConfig config) throws ClassNotFoundException, SQLException {
        Class.forName(JDBCConfig.m_jdbcClassName);
        m_connection = DriverManager.getConnection(config.getDatabaseURL(), config.getUserName(), config.getPassword());
    }

    public Connection getConnection() {
        return m_connection;
    }

    public void close() throws SQLException {
        if (m_connection != null) {
            m_connection.close();
        }
    }

    public void executeQuerySet(QuerySet querySet) throws SQLException {
        Statement stmt = m_connection.createStatement();
        for (String query : querySet.getQueries()) {
            if (printQuery) {
                System.out.println(query);
            }
            stmt.execute(query);
        }
    }
}
