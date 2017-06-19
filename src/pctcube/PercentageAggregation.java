package pctcube;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import pctcube.database.Column;
import pctcube.database.Database;
import pctcube.database.Table;

public class PercentageAggregation {

    public PercentageAggregation(Database db, Table factTable, Column measure) {
        m_database = db;
        m_factTable = factTable;
        m_measure = measure;
    }

    public void addTotalByKey(Column key) {
        m_totalByKeys.add(key);
    }

    public void addBreakdownByKey(Column key) {
        m_breakdownByKeys.add(key);
    }

    public List<Column> getTotalByKeys() {
        return Collections.unmodifiableList(m_totalByKeys);
    }

    public List<Column> getBreakdownByKeys() {
        return Collections.unmodifiableList(m_breakdownByKeys);
    }

    public Column getMeasure() {
        return m_measure;
    }

    public Table getFactTable() {
        return m_factTable;
    }

    protected Database getDatabase() {
        return m_database;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("Percentage aggregation on fact table '");
        builder.append(m_factTable.getTableName()).append("'\n");
        builder.append("MEASURE: ").append(m_measure.getQuotedColumnName());
        builder.append("\nTOTAL BY: ");
        for (Column c : m_totalByKeys) {
            builder.append(c.getQuotedColumnName()).append(",");
        }
        builder.setLength(builder.length() - 1);
        builder.append("\nBREAK DOWN BY: ");
        for (Column c : m_breakdownByKeys) {
            builder.append(c.getQuotedColumnName()).append(",");
        }
        builder.setLength(builder.length() - 1);
        return builder.toString();
    }

    private final List<Column> m_totalByKeys = new ArrayList<>();
    private final List<Column> m_breakdownByKeys = new ArrayList<>();
    private final Column m_measure;
    private final Database m_database;
    private final Table m_factTable;
}
