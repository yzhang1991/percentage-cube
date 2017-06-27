package pctcube;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

import pctcube.database.Column;
import pctcube.database.DataType;
import pctcube.database.DbConnection;
import pctcube.database.Table;

public class FactTableBuilder {

    private static final int MAX_GROUP_PER_DIMENSION = 5;

    private Table m_table;
    private String m_insertQuery;
    private String m_cubeParameter;

    public FactTableBuilder(String name, int dimensionCount) {
        if (dimensionCount <= 0) {
            throw new IllegalArgumentException("Dimension count should be at least 1.");
        }
        m_table = new Table(name);
        StringBuilder insertQueryBuilder = new StringBuilder("INSERT INTO ");
        StringBuilder paramBuilder = new StringBuilder("table=");
        paramBuilder.append(name).append(";dimensions=");
        insertQueryBuilder.append(name).append(" VALUES (");
        for (int i = 0; i < dimensionCount; i++) {
            String colName = "d" + i;
            m_table.addColumn(new Column(colName, DataType.VARCHAR));
            insertQueryBuilder.append("?,");
            paramBuilder.append(colName).append(",");
        }
        paramBuilder.setLength(paramBuilder.length() - 1);

        m_table.addColumn(new Column("m", DataType.FLOAT));
        paramBuilder.append(";measure=m");
        insertQueryBuilder.append("?);");
        m_insertQuery = insertQueryBuilder.toString();
        m_cubeParameter = paramBuilder.toString();
    }

    public Table getTable() {
        return m_table;
    }

    public String getCubeParameter() {
        return m_cubeParameter;
    }

    public void populateData(int rowCount, DbConnection conn) throws SQLException {
        Random rand = new Random();
        List<Column> columns = m_table.getColumns();
        int[] groupCounts = new int[m_table.getColumns().size() - 1];
        for (int i = 0; i < columns.size() - 1; i++) {
            groupCounts[i] = rand.nextInt(MAX_GROUP_PER_DIMENSION) + 1;
        }
        PreparedStatement insertStmt = conn.getConnection().prepareStatement(m_insertQuery);
        Statement stmt = conn.getConnection().createStatement();
        stmt.execute("TRUNCATE TABLE " + m_table.getTableName());
        for (int i = 0; i < rowCount; i++) {
            for (int j = 0; j < columns.size() - 1; j++) {
                insertStmt.setString(j + 1, // index start from 1
                        String.format("d%d_group%d", j, rand.nextInt(groupCounts[j])));
            }
            insertStmt.setFloat(columns.size(), rand.nextInt(100));
            insertStmt.addBatch();
            insertStmt.clearParameters();
            if (i % 200 == 0) {
                insertStmt.executeBatch();
            }
        }
        insertStmt.executeBatch();
    }
}
