package pctcube.database;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import pctcube.database.query.QuerySet;

public class DbConnection {

    private final Connection m_connection;
    private Statement m_stmt = null;
    private PrintStream m_sqlStream;

    public DbConnection() throws ClassNotFoundException, SQLException {
        this(new Config());
    }

    public DbConnection(Config config) throws ClassNotFoundException, SQLException {
        if (! config.isOffline()) {
            Class.forName(Config.m_jdbcClassName);
            m_connection = DriverManager.getConnection(config.getDatabaseURL(), config.getUserName(), config.getPassword());
            m_stmt = m_connection.createStatement();
        }
        else {
            m_connection = null;
        }
        m_sqlStream = config.getSQLStream();
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
        for (String query : querySet.getQueries()) {
            if (m_sqlStream != null) {
                m_sqlStream.println(query);
                m_sqlStream.println();
            }
            if (m_stmt != null) {
                m_stmt.execute(query);
            }
        }
    }

    public void execute(String query) throws SQLException {
        if (m_sqlStream != null) {
            m_sqlStream.println(query);
            m_sqlStream.println();
        }
        if (m_stmt != null) {
            m_stmt.execute(query);
        }
    }
}
