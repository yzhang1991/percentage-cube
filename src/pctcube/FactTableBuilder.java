package pctcube;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import pctcube.database.Column;
import pctcube.database.DataType;
import pctcube.database.DbConnection;
import pctcube.database.Table;

public class FactTableBuilder {

    private static final int MAX_GROUP_PER_DIMENSION = 10;

    private Table m_table;
    private String m_insertQuery;
    private String m_cubeParameter;
    private String m_projectionDDL;

    public FactTableBuilder(String name, int dimensionCount) {
        if (dimensionCount <= 0) {
            throw new IllegalArgumentException("Dimension count should be at least 1.");
        }
        m_table = new Table(name);
        StringBuilder insertQueryBuilder = new StringBuilder("INSERT INTO ");
        StringBuilder paramBuilder = new StringBuilder("table=");
        StringBuilder projectionQueryBuilder = new StringBuilder("CREATE PROJECTION ");
        List<String> columnNames = new ArrayList<>();
        paramBuilder.append(name).append(";dimensions=");
        insertQueryBuilder.append(name).append(" VALUES (");
        projectionQueryBuilder.append(name).append("_proj AS (SELECT * FROM ");
        projectionQueryBuilder.append(name).append(" ORDER BY ");
        for (int i = 0; i < dimensionCount; i++) {
            String colName = "d" + i;
            m_table.addColumn(new Column(colName, DataType.VARCHAR));
            columnNames.add(colName);
            insertQueryBuilder.append("?,");
        }
        paramBuilder.append(String.join(",", columnNames));
        projectionQueryBuilder.append(String.join(", ", columnNames));
        projectionQueryBuilder.append(");");
        m_table.addColumn(new Column("m", DataType.FLOAT));
        paramBuilder.append(";measure=m");
        insertQueryBuilder.append("?);");
        m_insertQuery = insertQueryBuilder.toString();
        m_cubeParameter = paramBuilder.toString();
        m_projectionDDL = projectionQueryBuilder.toString();
    }

    public Table getTable() {
        return m_table;
    }

    public String getCubeParameter() {
        return m_cubeParameter;
    }

    public String getProjectionDDL() {
        return m_projectionDDL;
    }

    public void populateData(int rowCount,
            int nullIn100,
            int[] cardinalities,
            DbConnection conn) throws SQLException {
        if (conn.getConnection() == null) {
            return;
        }
        List<Column> columns = m_table.getColumns();
        Random rand = new Random();
        if (cardinalities.length != columns.size()) {
            throw new RuntimeException("Not enough cardinalities are specified.");
        }
        PreparedStatement insertStmt = conn.getConnection().prepareStatement(m_insertQuery);
        Statement stmt = conn.getConnection().createStatement();
        stmt.execute("TRUNCATE TABLE " + m_table.getTableName());
        for (int i = 0; i < rowCount; i++) {
            for (int j = 0; j < columns.size() - 1; j++) {
                insertStmt.setString(j + 1, // index start from 1
                        String.format("d%d_group%d", j, rand.nextInt(cardinalities[j])));
            }
            if (rand.nextInt(100) >= nullIn100) {
                insertStmt.setFloat(columns.size(), rand.nextInt(100));
            }
            else {
                insertStmt.setNull(columns.size(), Types.FLOAT);
            }
            insertStmt.addBatch();
            insertStmt.clearParameters();
            if (i % 1000 == 0) {
                insertStmt.executeBatch();
            }
        }
        insertStmt.executeBatch();
    }


    public void populateData(int rowCount,
                             int nullIn100,
                             int cardinality,
                             DbConnection conn) throws SQLException {
        List<Column> columns = m_table.getColumns();
        Random rand = new Random();
        int[] cardinalities = new int[columns.size() - 1];
        for (int i = 0; i < columns.size() - 1; i++) {
            cardinalities[i] = cardinality == 0 ?
                               rand.nextInt(MAX_GROUP_PER_DIMENSION) + 1 :
                               cardinality;
        }
        populateData(rowCount, nullIn100, cardinalities, conn);
    }
}
