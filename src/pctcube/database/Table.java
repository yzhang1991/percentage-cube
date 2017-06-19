package pctcube.database;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import pctcube.Errors;
import pctcube.database.query.CreateTable;

public final class Table {

    public interface TableVisitor {
        void visit(Table table);
    }

    public Table(String name) {
        m_name = name;
    }

    public Table(Table copyFrom) {
        m_name = copyFrom.m_name;
        m_temporary = copyFrom.m_temporary;
        for (Column c : copyFrom.m_columns.values()) {
            addColumn(new Column(c));
        }
    }

    public void accept(TableVisitor visitor) {
        visitor.visit(this);
    }

    public List<Column> getColumns() {
        return new ArrayList<Column>(m_columns.values());
    }

    public void addColumn(Column column) {
        if (m_columns.containsKey(column.getColumnName())) {
            Errors.COLUMN_ALREADY_EXISTS.throwIt(null, column.getColumnName());
        }
        m_columns.put(column.getColumnName(), column);
        column.associateWithTable(this);
    }

    public Column getColumnByName(String name) {
        return m_columns.get(name);
    }

    public String getTableName() {
        return m_name;
    }

    public void setTableName(String name) {
        m_name = name;
    }

    public boolean isTempTable() {
        return m_temporary;
    }

    public void setTempTable(boolean value) {
        m_temporary = value;
    }

    @Override
    public String toString() {
        CreateTable visitor = new CreateTable();
        accept(visitor);
        return visitor.toString();
    }

    private String m_name;
    private final Map<String, Column> m_columns = new LinkedHashMap<>();
    private boolean m_temporary = false;
}
